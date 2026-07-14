# Secondary Index V2 Production Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to execute this plan task by task. Keep the checkboxes current and
> stop at every review checkpoint.

**Goal:** Close production-readiness review issues 1-5 with the smallest changes inside existing
client, replica-index lifecycle, metric, accumulator, and test ownership boundaries.

**Design source:**
`docs/superpowers/specs/2026-07-14-secondary-index-v2-production-hardening-design.md`

**Architecture:** The upgraded public Table API rejects writer creation for system-managed Index
Tables. Existing source-bucket `IndexReplicator` instances report one-shot terminal failure to
their current `ReplicaIndexController`, which exposes one constant-cardinality TabletServer gauge.
The existing TabletServer-global `IndexAccumulator` retains its per-replicator threshold and adds
an exact whole-window total admission bound based on retained 4 KiB payload pages. Existing
WriterState fencing, target retry, source-WAL progress, and SYNC/ASYNC semantics are unchanged.

**Tech Stack:** Java, Maven, Fluss Table API, KV/WAL replication, JUnit 5, AssertJ, Mockito,
RocksDB-backed integration fixtures, remote log tiering.

## Global Constraints

- Do not add a new RPC, request marker, caller token, principal, Coordinator/ZooKeeper record,
  listener subsystem, scheduler, waiter queue, or per-bucket metric.
- Do not use `ApiVersion` as caller identity. PutKv API v2 remains only a transport-capability
  check for fenced V1 batches.
- Trust the upgraded public client. A handcrafted valid V1 PutKv that bypasses `Table.newUpsert()`
  remains outside this phase's supported boundary.
- Target loss, target leader migration, stale metadata, transport timeout, and rolling-upgrade
  incompatibility remain retryable for both SYNC and ASYNC indexes. Visibility waiting is their
  only difference.
- Terminal failure is reserved for source state that cannot make progress by retrying, including
  source-WAL corruption and a window that can never fit the configured total retained-byte bound.
- Keep `index.replication.max-pending-bytes` as the per-replicator threshold. A valid indivisible
  window may take an owner slightly above it.
- Add `index.replication.max-total-pending-bytes` as the hard post-admission TabletServer bound,
  default `256mb`.
- Capacity admission is for one complete `IndexWindow`; partial batch admission is forbidden.
- `indexPushPendingBytes` measures admitted, unreleased retained payload pages, including queued,
  in-flight, and retry-retained batches. It is not an exact JVM-heap measurement.
- Preserve the existing lock order: replicator lifecycle -> window -> batch/target queue. Never
  call `IndexReplicator.close()` while holding the controller lifecycle monitor.
- Do not add fixed `Thread.sleep` synchronization. Use latches, deterministic hooks, futures, and
  condition waits.
- Do not touch lake-table behavior or review issue 6.
- Preserve unrelated user changes. Do not use broad reset, checkout, restore, or clean commands.
- Every implementation commit is concise and has no coauthor or tool-identification trailer.

## Interface Map

- `FlussTable.newUpsert()` owns the upgraded public-client Index Table write boundary.
- `IndexReplicator` gains a standard `BiConsumer<IndexReplicator, Throwable>` terminal callback;
  no new listener interface is introduced.
- `ReplicaIndexController` gains `FAILED`, exact-replicator identity checks, and `isFailed()`.
- `ReplicaManager` aggregates current failed leader source buckets at scrape time.
- `TabletServerMetricGroup` owns the constant-cardinality
  `indexReplicationFailedSourceBucketCount` gauge.
- `IndexBatch.retainedBytes()` records allocator-backed retained payload bytes separately from
  logical encoded length.
- `IndexAccumulator.tryAppendWindow(List<IndexBatch>)` is the sole production admission boundary.
- `IndexWindow` prevents sender visibility until every batch has been published and accounting has
  committed.
- `IndexReplicator` keeps source progress unchanged when admission rejects and terminally fails an
  individually impossible window.

## Requirement Coverage

| Requirement | Owning task |
|---|---|
| Public client blocks Index Table upsert without ApiVersion policy | Task 1 |
| One-shot terminal handoff and stale-instance race protection | Task 2 |
| Persistent server-level failed-source health signal | Task 3 |
| Page-accurate retained payload accounting | Task 4 |
| Whole-window total capacity and release/retry correctness | Task 5 |
| Causally valid target-leader failover retry test | Task 6 |
| Raw remote source-WAL replay across source leader failover | Task 7 |
| FIP/config/metric cleanup and full branch verification | Task 8 |

---

## Task 1: Reject Public Upsert Writers For Index Tables

**Files:**

- Modify: `fluss-client/src/main/java/org/apache/fluss/client/table/FlussTable.java`
- Modify:
  `fluss-client/src/test/java/org/apache/fluss/client/table/FlussTableSecondaryIndexITCase.java`

**Interfaces:**

- Consumes: `TableInfo.isIndexTable()` on the metadata already held by `FlussTable`.
- Produces: immediate `IllegalStateException` from `FlussTable.newUpsert()` for Index Tables;
  creates no writer and sends no RPC.

### 1.1 Write the failing client integration test

- [ ] Add `indexTableRejectsPublicUpsertWriterCreation()` to the existing secondary-index Table
  API fixture. Create a real main table and let the Coordinator derive its Index Table.
- [ ] Assert the derived handle reports `getTableInfo().isIndexTable()`.
- [ ] Assert Index Table row count is zero before writer creation, `newUpsert()` throws immediately,
  and row count remains zero afterward.
- [ ] Assert the main table still accepts `newUpsert()` so the check is scoped by table metadata.
- [ ] Do not add an ApiVersion loop or raw-RPC test; those would encode the rejected access-control
  design.

Use this test shape:

```java
@Test
void indexTableRejectsPublicUpsertWriterCreation() throws Exception {
    TablePath mainPath = TablePath.of(DB, "test_public_index_write_guard");
    Schema schema =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .index(
                            "idx_name",
                            IndexType.SECONDARY,
                            Collections.singletonList("name"),
                            IndexVisibility.SYNC,
                            2)
                    .build();
    createTable(
            mainPath,
            TableDescriptor.builder().schema(schema).distributedBy(2, "id").build(),
            true);

    TablePath indexPath =
            TablePath.of(
                    DB,
                    IndexTableUtils.indexTableName(mainPath.getTableName(), "idx_name"));
    try (Table mainTable = conn.getTable(mainPath);
            Table indexTable = conn.getTable(indexPath)) {
        assertThat(indexTable.getTableInfo().isIndexTable()).isTrue();
        assertThat(admin.getTableStats(indexPath).get().getRowCount()).isZero();

        assertThatThrownBy(indexTable::newUpsert)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(indexPath.toString())
                .hasMessageContaining("internal secondary index table");

        assertThat(admin.getTableStats(indexPath).get().getRowCount()).isZero();
        assertThat(mainTable.newUpsert()).isNotNull();
    }
}
```

### 1.2 Run the focused test and verify red

- [ ] Run:

```bash
cd /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-index-v2
mvn -o -Dmaven.repo.local=.cache -pl fluss-client -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=FlussTableSecondaryIndexITCase test
```

Expected: the new assertion fails because the derived Index Table currently returns a
`TableUpsert`.

### 1.3 Implement the metadata guard at the public boundary

- [ ] Add the Index Table check before the existing primary-key check:

```java
@Override
public Upsert newUpsert() {
    checkState(
            !tableInfo.isIndexTable(),
            "Table %s is an internal secondary index table and doesn't support public UpsertWriter.",
            tablePath);
    checkState(
            hasPrimaryKey,
            "Table %s is not a Primary Key Table and doesn't support UpsertWriter.",
            tablePath);
    return new TableUpsert(tablePath, tableInfo, conn.getOrCreateWriterClient());
}
```

- [ ] Do not change `newAppend()`: Index Tables already have primary keys and are rejected by its
  existing Log Table check.
- [ ] Do not change PutKv RPC handling, `IndexKvWriteGuard`, or API negotiation.

### 1.4 Verify and commit

- [ ] Re-run `FlussTableSecondaryIndexITCase` and confirm green.
- [ ] Run `git diff --check`.
- [ ] Commit:

```bash
git add fluss-client/src/main/java/org/apache/fluss/client/table/FlussTable.java \
  fluss-client/src/test/java/org/apache/fluss/client/table/FlussTableSecondaryIndexITCase.java
git commit -m "Block public writes to index tables"
```

---

## Task 2: Persist Terminal Replicator Failure In The Current Controller

**Files:**

- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java`
- Modify:
  `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorLifecycleTest.java`
- Create:
  `fluss-server/src/test/java/org/apache/fluss/server/index/ReplicaIndexControllerTest.java`

**Interfaces:**

- Consumes: the existing authoritative `IndexReplicator.terminalFailure()` CAS and cleanup path.
- Produces: callback-aware `IndexReplicator` construction,
  `ReplicaIndexController.State.FAILED`, `isFailed()`, `installIndexReplicator(...)`, and
  `onIndexReplicatorFailed(IndexReplicator, Throwable)`.

### 2.1 Write one-shot callback tests first

- [ ] Extend the existing terminal source-WAL corruption fixture to pass a callback and assert all
  of the following together:
  - the callback receives the exact `IndexReplicator` instance;
  - it receives the exact authoritative terminal cause;
  - it fires once after source reader/read-context cleanup;
  - a second `poll()` and repeated `close()` do not fire it again.
- [ ] Add a callback-throws case. The callback exception must be suppressed/logged while the
  original terminal cause remains in `terminalFailure()`, cleanup still completes, and no owned
  accounting survives.

Add a callback-aware test helper overload instead of rewriting existing callers:

```java
private static IndexReplicator replicator(
        IndexSourceReader reader,
        IndexAccumulator accumulator,
        LogRecordReadContext readContext,
        BiConsumer<IndexReplicator, Throwable> onTerminalFailure) {
    return IndexReplicator.forTesting(
            reader,
            Collections.singletonList(spec("idx")),
            accumulator,
            readContext,
            0L,
            1024,
            1024,
            (sync, all) -> {},
            onTerminalFailure);
}
```

Strong assertions:

```java
assertThat(callbackCount).hasValue(1);
assertThat(reportedReplicator.get()).isSameAs(replicator);
assertThat(reportedFailure.get()).isSameAs(replicator.terminalFailure());
verify(readContext, times(1)).close();
assertThat(accumulator.pendingBytes(replicator)).isZero();
```

### 2.2 Write controller identity and lifecycle tests

- [ ] Create `ReplicaIndexControllerTest` in the same package so it can exercise the production
  lifecycle helpers without reflection.
- [ ] Use a mocked `IndexReplicatorPool`, a real `IndexAccumulator`, and lightweight real test
  replicators.
- [ ] Prove this sequence exactly:

```text
install first       -> RUNNING
first reports error -> FAILED
become follower     -> NOT_STARTED and first detached
install replacement -> RUNNING
late first error    -> still RUNNING with replacement current
replacement error  -> FAILED
close               -> NOT_STARTED
```

Use identity assertions, not only enum assertions:

```java
controller.installIndexReplicator(first);
controller.onIndexReplicatorFailed(first, firstFailure);
assertThat(controller.getState()).isEqualTo(State.FAILED);
assertThat(controller.isFailed()).isTrue();

controller.onBecomeFollower();
controller.installIndexReplicator(replacement);
controller.onIndexReplicatorFailed(first, new RuntimeException("late old callback"));
assertThat(controller.getState()).isEqualTo(State.RUNNING);
assertThat(controller.getIndexReplicator()).isSameAs(replacement);
```

- [ ] Verify `replicatorPool.unregister(tableBucket)` happens before close/drop and repeated stop is
  idempotent.
- [ ] Add `terminalCallbackMovesInstalledControllerToFailed()` using one real test replicator whose
  callback is the exact method reference `controller::onIndexReplicatorFailed`. Install that same
  instance, trigger the existing corrupt-source-WAL terminal path, and assert the callback arrives
  only after cleanup and changes the controller from `RUNNING` to `FAILED`. This fails if either
  callback delivery or the controller's identity-fenced handoff behavior is broken.

### 2.3 Run the new tests and verify red

- [ ] Run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexReplicatorLifecycleTest,ReplicaIndexControllerTest test
```

Expected: compilation fails because callback overloads, `FAILED`, `isFailed()`, and controller
lifecycle helpers do not exist.

### 2.4 Implement the one-shot terminal handoff

- [ ] Add a no-op standard-library callback field to `IndexReplicator`:

```java
private static final BiConsumer<IndexReplicator, Throwable> NO_TERMINAL_CALLBACK =
        (ignoredReplicator, ignoredFailure) -> {};
private final BiConsumer<IndexReplicator, Throwable> onTerminalFailure;
```

- [ ] Preserve every existing constructor and `forTesting` signature by delegating to the new
  callback-aware constructor with `NO_TERMINAL_CALLBACK`.
- [ ] Add only the one package-visible constructor/`forTesting` overload needed by production and
  focused tests.
- [ ] Invoke the callback only after the terminal CAS wins and cleanup completes:

```java
private void transitionToTerminalLocked(Throwable failure) {
    if (!terminalFailure.compareAndSet(null, failure)) {
        return;
    }
    Throwable cleanupFailure = cleanupOwnedResourcesLocked();
    if (cleanupFailure != null && cleanupFailure != failure) {
        failure.addSuppressed(cleanupFailure);
    }
    LOG.error(
            "Index replication for source bucket {} failed terminally at pushed offset {}",
            sourceReader.tableBucket(),
            getAllIndexPushedOffset(),
            failure);
    try {
        onTerminalFailure.accept(this, failure);
    } catch (Throwable callbackFailure) {
        if (callbackFailure != failure) {
            failure.addSuppressed(callbackFailure);
        }
        LOG.warn(
                "Failed to report terminal index replication state for {}",
                sourceReader.tableBucket(),
                callbackFailure);
    }
}
```

The callback runs under the replicator lifecycle lock. Its production implementation may only
record controller state; it must not close the replicator, perform RPC, or call back into it.

### 2.5 Implement controller state with exact-instance fencing

- [ ] Add `FAILED` and update the class state-machine Javadoc.
- [ ] Add a private controller lifecycle monitor and make `indexReplicator` `volatile` for lock-free
  query visibility.
- [ ] Route production start through package-visible `installIndexReplicator(...)`, and terminal
  reporting through package-visible `onIndexReplicatorFailed(...)`. These are lifecycle units used
  by production, not a new public subsystem.
- [ ] Wire the callback from the production constructor to
  `this::onIndexReplicatorFailed`.
- [ ] Under the controller monitor, install the current instance and `RUNNING` state before pool
  registration can let a worker poll it. Roll back both fields if registration throws, then close
  the uninstalled replicator outside the monitor.
- [ ] Capture the pre-install state and restore that exact state if pool registration throws
  (`DEFERRED` remains retryable; `NOT_STARTED` remains stopped). Never leave a failed registration
  looking `RUNNING`.
- [ ] Implement the identity-fenced terminal transition:

```java
void onIndexReplicatorFailed(IndexReplicator failed, Throwable failure) {
    synchronized (lifecycleLock) {
        if (indexReplicator != failed || state.get() != State.RUNNING) {
            return;
        }
        state.set(State.FAILED);
    }
    LOG.error("Index replication stopped for source bucket {}", tableBucket, failure);
}

public boolean isFailed() {
    return state.get() == State.FAILED;
}
```

- [ ] Refactor stop into two phases:
  1. under the controller monitor, detach the exact current instance and set `NOT_STARTED`;
  2. outside the monitor, unregister, close, and drop its queued batches.
- [ ] Keep `retryStart()` limited to `DEFERRED`; never turn the same failed run back into
  `RUNNING`.

### 2.6 Verify and commit

- [ ] Re-run the two focused test classes plus `ReplicaTest`, whose existing indexed-replica cases
  cover deferred metadata startup and leader-epoch replacement.
- [ ] Run `git diff --check`.
- [ ] Commit:

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorLifecycleTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/ReplicaIndexControllerTest.java
git commit -m "Persist terminal index replication state"
```

---

## Task 3: Add One Constant-Cardinality Failure Gauge

**Files:**

- Modify: `fluss-common/src/main/java/org/apache/fluss/metrics/MetricNames.java`
- Modify:
  `fluss-server/src/main/java/org/apache/fluss/server/metrics/group/TabletServerMetricGroup.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`
- Modify:
  `fluss-server/src/test/java/org/apache/fluss/server/metrics/group/TabletServerMetricGroupTest.java`

**Interfaces:**

- Consumes: Task 2 `ReplicaIndexController.isFailed()` and the existing local `onlineReplicas()`
  stream.
- Produces: `MetricNames.INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT` and a fourth
  `LongSupplier` in the existing index-push gauge registration.

### 3.1 Write gauge ownership tests first

- [ ] Extend `testWriterStateGaugeOwnershipCanBeReplacedAndCleared()` with first and second failed
  source-bucket suppliers.
- [ ] Assert the gauge follows replacement ownership, ignores closing an old registration, and
  returns zero when the current registration closes.
- [ ] Assert there is only one metric named
  `indexReplicationFailedSourceBucketCount`; add no table/bucket labels.

Update the helper shape to include the fourth value:

```java
private static void assertPushGauges(
        TabletServerMetricGroup metrics,
        long expectedPending,
        long expectedInFlight,
        long expectedAge,
        long expectedFailedSources) {
    assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_PENDING_BYTES))
            .isEqualTo(expectedPending);
    assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_IN_FLIGHT_REQUESTS))
            .isEqualTo(expectedInFlight);
    assertThat(metricValue(metrics, MetricNames.INDEX_PUSH_OLDEST_IN_FLIGHT_AGE_MS))
            .isEqualTo(expectedAge);
    assertThat(
                    metricValue(
                            metrics,
                            MetricNames.INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT))
            .isEqualTo(expectedFailedSources);
}
```

### 3.2 Run the focused test and verify red

- [ ] Run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=TabletServerMetricGroupTest test
```

Expected: compilation fails because the metric name and fourth supplier do not exist.

### 3.3 Implement the aggregate supplier

- [ ] Add exactly:

```java
public static final String INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT =
        "indexReplicationFailedSourceBucketCount";
```

- [ ] Remove the unused `INDEX_PUSHED_OFFSET_LAG` constant. `rg` must show no production or test
  reference before removal.
- [ ] Extend `IndexPushGaugeSource`, `EMPTY`, `registerIndexPushGauges(...)`, and the constructor
  gauge registration with a fourth `LongSupplier`.
- [ ] Register the `ReplicaManager` supplier with the existing push gauges:

```java
this.indexPushGaugeRegistration =
        serverMetricGroup.registerIndexPushGauges(
                indexAccumulator::pendingBytes,
                indexSender::inFlightRequestCount,
                indexSender::oldestInFlightAgeMs,
                this::failedIndexReplicationSourceBucketCount);
```

- [ ] Compute the value from the existing local replica map at scrape time:

```java
private long failedIndexReplicationSourceBucketCount() {
    return onlineReplicas()
            .filter(Replica::isLeader)
            .filter(replica -> replica.getIndexManager().isFailed())
            .count();
}
```

This counts only current leader source buckets. A follower transition detaches the failed
replicator and naturally removes it from the gauge.

### 3.4 Verify and commit

- [ ] Run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=TabletServerMetricGroupTest,ReplicaIndexControllerTest,ReplicaManagerTest test
```

- [ ] Run `git diff --check`.
- [ ] Commit:

```bash
git add fluss-common/src/main/java/org/apache/fluss/metrics/MetricNames.java \
  fluss-server/src/main/java/org/apache/fluss/server/metrics/group/TabletServerMetricGroup.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java \
  fluss-server/src/test/java/org/apache/fluss/server/metrics/group/TabletServerMetricGroupTest.java
git commit -m "Expose terminal index replication failures"
```

---

## Task 4: Account Retained Payload Pages Instead Of Logical Length

**Files:**

- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexBatch.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexAccumulator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java`
- Modify:
  `fluss-server/src/test/java/org/apache/fluss/server/index/IndexAccumulatorTest.java`
- Modify:
  `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java`

**Interfaces:**

- Consumes: `UnmanagedPagedOutputView.getWrittenSegments()` and the existing 4096-byte index
  builder page size.
- Produces: `IndexBatch.retainedBytes()`; request payload sizing remains
  `encoded().getBytesLength()`.

### 4.1 Write retained-byte tests first

- [ ] Extend the `IndexAccumulatorTest.batch(...)` helper with an explicit retained-byte overload.
- [ ] Prove pending accounting uses retained bytes while request payload remains encoded length:

```java
@Test
void pendingAccountingUsesRetainedBytes() {
    IndexAccumulator accumulator = new IndexAccumulator();
    IndexReplicator owner = replicator(accumulator);
    IndexWindow window = new IndexWindow("idx", 10L, 1, owner);
    IndexBatch batch = batch(new TableBucket(700L, 0), window, 3, 4096L);

    accumulator.append(batch);

    assertThat(batch.encoded().getBytesLength()).isEqualTo(3);
    assertThat(batch.retainedBytes()).isEqualTo(4096L);
    assertThat(accumulator.pendingBytes()).isEqualTo(4096L);
    assertThat(accumulator.pendingBytes(owner)).isEqualTo(4096L);

    accumulator.release(accumulator.pollFirst(batch.targetBucket()));
    assertThat(accumulator.pendingBytes()).isZero();
}
```

- [ ] Add validation for retained bytes smaller than encoded length and for negative values.
- [ ] In `IndexReplicatorAppendTest`, build one tiny production `BucketBatchBuilder`, finish it, and
  assert logical bytes are below 4096 while `retainedBytes()` is exactly 4096.

### 4.2 Run the focused tests and verify red

- [ ] Run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexAccumulatorTest,IndexReplicatorAppendTest test
```

Expected: compilation fails because `IndexBatch.retainedBytes()` and the explicit constructor do
not exist.

### 4.3 Implement explicit retained-byte ownership

- [ ] Add the field and constructor while preserving existing test helpers:

```java
private final long retainedBytes;

IndexBatch(TableBucket targetBucket, BytesView encoded, IndexWindow window) {
    this(targetBucket, encoded, encoded.getBytesLength(), window);
}

IndexBatch(
        TableBucket targetBucket,
        BytesView encoded,
        long retainedBytes,
        IndexWindow window) {
    this.targetBucket = checkNotNull(targetBucket, "targetBucket");
    this.encoded = checkNotNull(encoded, "encoded");
    checkArgument(
            retainedBytes >= encoded.getBytesLength(),
            "retainedBytes must cover the encoded payload");
    this.retainedBytes = retainedBytes;
    this.window = checkNotNull(window, "window");
    // Keep the existing one-shot state initialization and window.register(this).
}

long retainedBytes() {
    return retainedBytes;
}
```

The three-argument constructor describes arbitrary test `BytesView` allocations and therefore
defaults to exact encoded length. Production passes the allocator-backed page count explicitly.

- [ ] Replace both accumulator accounting and release calculations with
  `batch.retainedBytes()`.
- [ ] Keep logical encoded length for PutKv request sizing and
  `IndexWindow.registeredPayloadBytes()`.
- [ ] Store the `UnmanagedPagedOutputView` in `BucketBatchBuilder` and expose:

```java
long retainedBytes() {
    return Math.multiplyExact((long) output.getWrittenSegments().size(), PAGE_SIZE);
}
```

- [ ] When the production replicator creates each `IndexBatch`, pass the matching builder's
  retained page bytes. Do not round request bytes or introduce a generic `BytesView` allocation
  abstraction.

### 4.4 Verify and commit

- [ ] Re-run `IndexAccumulatorTest`, `IndexReplicatorAppendTest`, and `IndexSenderTest`.
- [ ] Run `git diff --check`.
- [ ] Commit:

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/index/IndexBatch.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexAccumulator.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexAccumulatorTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java
git commit -m "Account retained index batch pages"
```

---

## Task 5: Add Atomic Whole-Window Total Capacity

**Files:**

- Modify: `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexWindow.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexBatch.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexAccumulator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexWindowTest.java`
- Modify:
  `fluss-server/src/test/java/org/apache/fluss/server/index/IndexAccumulatorTest.java`
- Modify:
  `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSenderTest.java`

**Interfaces:**

- Consumes: Task 2 terminal callback and Task 4 `IndexBatch.retainedBytes()`.
- Produces: `INDEX_REPLICATION_MAX_TOTAL_PENDING_BYTES`,
  `IndexAccumulator(long, long)`, `tryAppendWindow(List<IndexBatch>)`, and
  `IndexWindow.isAdmitted()`.

### 5.1 Write accumulator admission tests first

- [ ] Add `IndexAccumulator(long maxPendingBytes, long maxTotalPendingBytes)` test coverage for
  positive validation and preserve these constructors:

```java
IndexAccumulator()                         // per owner = MAX, total = MAX
IndexAccumulator(long maxPendingBytes)     // legacy tests: per owner = value, total = value
IndexAccumulator(long perOwner, long total)
```

- [ ] Add a two-target all-or-none test. Owner A fills the total bound; owner B's entire two-batch
  window must return `false`, publish no target queue, account zero owner bytes, and fire no append
  callback.
- [ ] Add a concurrent-admission test with two owners, a barrier, and capacity for exactly one
  window. Exactly one call returns true and `pendingBytes()` never exceeds the total bound.
- [ ] Add a tiny-fanout test where logical bytes fit but page-retained bytes exceed the total, proving
  the total check uses Task 4 accounting.
- [ ] Add retry/release tests proving re-enqueue does not reserve again and ACK, terminal failure,
  and owner close each release exactly once.
- [ ] Retain and adapt the existing append-vs-owner-close hook test; after a close wins during
  admission, total and per-owner accounting must both return to zero and no queue entry may escape.

Central assertions:

```java
assertThat(accumulator.tryAppendWindow(ownerAWindow)).isTrue();
assertThat(accumulator.pendingBytes()).isEqualTo(totalLimit);

assertThat(accumulator.tryAppendWindow(ownerBWindow)).isFalse();
assertThat(accumulator.pendingBytes(ownerB)).isZero();
assertThat(accumulator.hasPending(ownerBTarget0)).isFalse();
assertThat(accumulator.hasPending(ownerBTarget1)).isFalse();
assertThat(wakeups).hasValue(ownerAWindow.size());
```

### 5.2 Write replicator admission and impossible-window tests

- [ ] Add a total-full but individually valid case:
  1. another owner fills total capacity;
  2. the source replicator reads/encodes a window but admission returns false;
  3. no in-flight window survives and pushed offset is unchanged;
  4. after releasing capacity, polling the same source offset succeeds and reaches the exact window
     end.
- [ ] Add a production-page case with total capacity below 4096. One tiny derived window must enter
  terminal `RecordTooLargeException` once, retain zero pending bytes, leave no in-flight window,
  and never reread on a second `poll()`.
- [ ] Assert the terminal callback from Task 2 fires for the impossible window.
- [ ] Preserve `publishesWindowStateBeforeSynchronousAccumulatorCallbacks()`: an immediate sender
  completion must not observe a missing source in-flight window.

### 5.3 Write a sender retention/recovery test

- [ ] Add a focused `IndexSenderTest` using its existing `RecordingGateway`:
  1. admit windows up to the exact total bound;
  2. leave PutKv futures unresolved and assert queued/in-flight retained bytes remain at the bound;
  3. reject another owner's window;
  4. fail and re-enqueue one request and assert no double accounting;
  5. recover the gateway, ACK the admitted windows, and assert the total drains to zero;
  6. rebuild the previously rejected source window, admit it, and assert exact pushed-offset
     completion.

This test proves pressure behavior through the real sender state machine rather than only through
queue bookkeeping.

### 5.4 Run the new tests and verify red

- [ ] Run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexAccumulatorTest,IndexReplicatorAppendTest,IndexReplicatorLifecycleTest,IndexSenderTest test
```

Expected: compilation fails because the two-limit constructor and whole-window API do not exist.

### 5.5 Add the total-capacity option and production wiring

- [ ] Add exactly:

```java
public static final ConfigOption<MemorySize> INDEX_REPLICATION_MAX_TOTAL_PENDING_BYTES =
        key("index.replication.max-total-pending-bytes")
                .memoryType()
                .defaultValue(MemorySize.parse("256mb"))
                .withDescription(
                        "Maximum retained payload bytes of admitted, unacknowledged index "
                                + "batches across one TabletServer. The value includes queued, "
                                + "in-flight, and retry-retained page buffers. It is a hard "
                                + "post-admission bound for accumulator payload pages, not an "
                                + "exact bound on total JVM heap usage.");
```

- [ ] Update the existing per-owner option description to say `retained payload bytes` and explain
  that one indivisible window may cross its threshold.
- [ ] Construct the production accumulator with both options in `ReplicaManager`.

### 5.6 Implement staged whole-window publication

- [ ] Add `maxTotalPendingBytes` and a small private `admissionLock` to `IndexAccumulator`.
- [ ] Keep `AtomicLong pendingBytes` so release does not wait for admission. Serialize only competing
  reserve/check operations under `admissionLock`.
- [ ] Add `volatile boolean admitted` to `IndexWindow`. New windows start unadmitted; successful
  accumulator publication marks them admitted only after every target queue contains its batch.
- [ ] Make both `pollFirst(...)` and `pollFirstReady(...)` leave an unadmitted head in place. This
  prevents an RPC from escaping while another target batch of the same window is still being
  published or while rollback is possible.
- [ ] Keep `IndexWindow.isActive()` as the terminal/retirement predicate. Add a separate
  `isAdmitted()` predicate instead of overloading `active` with two meanings.
- [ ] Validate the input to `tryAppendWindow(...)`:
  - non-empty list;
  - every batch references the exact same window instance;
  - the list size equals the window's immutable expected batch count;
  - the list contains every registered batch for that window;
  - no duplicate target bucket;
  - no batch is released or already accounted;
  - owner/window is still active.

Use this public shape:

```java
public boolean tryAppendWindow(List<IndexBatch> windowBatches)
```

- [ ] Implement the critical sequence:

```text
lock window
  validate and sum retained bytes with overflow detection
  if windowBytes > maxTotal: throw RecordTooLargeException
  lock admissionLock
    if total + windowBytes > maxTotal: return false
    reserve total and owner accounting; mark every batch accounted
  unlock admissionLock
  run existing deterministic admission hook; recheck owner
  publish every batch to its target queue while window remains unadmitted
  mark window admitted
unlock window
notify target sender workers
```

- [ ] If the hook throws, the owner closes after reservation, or queue publication throws before
  activation, remove every staged queue entry and release the single reservation before returning
  or rethrowing. Because sender polling rejects unadmitted batches, rollback has exclusive ownership
  of those entries.
- [ ] Store the immutable expected batch count separately from `remaining` in `IndexWindow`; expose
  it package-locally for admission validation.
- [ ] Keep `append(IndexBatch)` only as a compatibility helper for one-batch test windows,
  implemented through singleton `tryAppendWindow`. It must reject a multi-batch window rather than
  accidentally activating its first batch alone. Convert every existing multi-batch setup in
  `IndexSenderTest` to construct all batches first and call one
  `tryAppendWindow(Arrays.asList(...))`. Production `IndexReplicator` must use only the
  whole-window API.
- [ ] Append-listener callbacks stay outside window/admission/queue locks. Record missed
  notifications exactly as today.

### 5.7 Integrate source progress without an ACK race

- [ ] Add the cheap precheck to `pollLocked()` and its per-index loop:

```java
if (accumulator.isFull() || accumulator.isFull(this)) {
    return false;
}
```

`isFull()` now means total capacity; `isFull(owner)` remains the per-owner threshold.

- [ ] Under the already-held replicator lifecycle lock, create and transiently register the
  in-flight window before calling the accumulator. This preserves the existing protection against
  a synchronous ACK overtaking source state publication.
- [ ] On `false`, clear only that exact window and retire its unadmitted batches before returning.
  Observably, the rejected poll leaves no in-flight window and no progress, satisfying the design's
  no-registration-on-rejection contract.
- [ ] On `RecordTooLargeException`, clear/retire the exact unadmitted window, call
  `transitionToTerminalLocked(failure)`, and return false. The second poll must stop at the terminal
  check without rereading.
- [ ] On any other publication exception, clear/retire the exact window before propagating; never
  leave a source index permanently blocked on a window that was not admitted.

### 5.8 Verify and commit

- [ ] Run the focused tests plus the existing pool/window suites:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexAccumulatorTest,IndexWindowTest,IndexSourceReaderTest,IndexReplicatorAppendTest,IndexReplicatorLifecycleTest,IndexSenderTest test
```

- [ ] Run `git diff --check`.
- [ ] Commit:

```bash
git add fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexWindow.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexBatch.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexAccumulator.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexWindowTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexAccumulatorTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexSenderTest.java
git commit -m "Bound retained index push buffers"
```

---

## Task 6: Make Target Failover Retry Test Causally Valid

**Files:**

- Modify:
  `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushFailoverITCase.java`

**Interfaces:**

- Consumes: existing cluster leader lookup, source PutKv, physical index-key encoder, and
  condition-wait helpers.
- Produces: no production interface; replaces a topology-ambiguous test with a causal retry test.

### 6.1 Rewrite topology and data selection together

- [ ] In `testIndexTableLeaderFailoverRetries()`, inspect the three actual Index Table bucket
  leaders and select a bucket whose leader differs from the source main-table leader.
- [ ] Fail setup with a diagnostic if no such topology exists; never silently switch only the
  server variable.
- [ ] Find a deterministic string that hashes to the selected bucket:

```java
private static String valueForIndexBucket(String prefix, int expectedBucket) {
    for (int suffix = 0; suffix < 100_000; suffix++) {
        String candidate = prefix + '-' + suffix;
        if (computeIndexBucket(candidate) == expectedBucket) {
            return candidate;
        }
    }
    throw new AssertionError("No value found for index bucket " + expectedBucket);
}
```

- [ ] Derive `indexKey`, `targetIdxBucket`, and `TableBucket idxTb` from that same selected value.

### 6.2 Exercise the actual retry interval

- [ ] Stop the exact current leader of `idxTb` and assert it is not the source leader.
- [ ] Submit the SYNC source write immediately, keeping its future unresolved while target
  leadership changes. Do not wait for every Index Table bucket to elect before submitting.
- [ ] Wait only for `idxTb` to acquire a different leader, then require the source write future to
  complete.
- [ ] Assert the exact physical index key is present on the new target leader and source sync pushed
  offset is exactly `1L`.
- [ ] Put server restart and cluster-health restoration in `finally` so a failed assertion cannot
  poison later tests.
- [ ] Set `boolean targetLeaderStopped` only after `stopTabletServer(...)` returns. In `finally`,
  restart only when that flag is true, then wait for three healthy TabletServers. Preserve the
  primary test failure by adding any restart failure as suppressed.

The key order must be:

```java
FLUSS_CLUSTER_EXTENSION.stopTabletServer(stoppedTargetLeader);
CompletableFuture<PutKvResponse> sourceWrite =
        FLUSS_CLUSTER_EXTENSION
                .newTabletServerClientForNode(mainLeader)
                .putKv(
                        newPutKvRequest(
                                mainTableId,
                                0,
                                1,
                                genKvRecordBatch(new Object[] {1, selectedValue})));
waitUntil(
        () -> FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(idxTb) != stoppedTargetLeader,
        TIMEOUT,
        "wait for the selected target index bucket to fail over");
sourceWrite.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
```

### 6.3 Verify and commit

- [ ] Run the test repeatedly to expose topology assumptions:

```bash
for run in 1 2 3; do
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
    -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
    -Dtest=IndexPushFailoverITCase#testIndexTableLeaderFailoverRetries test || exit 1
done
```

- [ ] Confirm `rg -n 'Thread\.sleep'` finds no synchronization in this class.
- [ ] Run `git diff --check`.
- [ ] Commit:

```bash
git add fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushFailoverITCase.java
git commit -m "Strengthen index target failover coverage"
```

---

## Task 7: Prove Raw Remote Source-WAL Recovery End To End

**Files:**

- Create:
  `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceRemoteRecoveryITCase.java`
- Reuse without production modification:
  `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTestHooks.java`

**Interfaces:**

- Consumes: existing raw remote-log tiering, `IndexReplicatorPool.unregister(...)`, scoped
  `ReplicaTestHooks`, `indexSourceRemoteReadBytes`, and physical Index Table encoders.
- Produces: no production interface; adds end-to-end evidence for snapshot restore, remote replay,
  failover, exact target state, and local continuation.

### 7.1 Build a four-role deterministic fixture

- [ ] Use four TabletServers with default replication factor three and these settings copied from
  the proven KV remote-recovery fixture:

```java
conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("100b"));
conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
conf.setInt(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS, 1);
conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1b"));
conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
conf.set(ConfigOptions.INDEX_REPLICATION_BACKOFF_INTERVAL, Duration.ofMillis(5));
conf.set(ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF, Duration.ofMillis(5));
```

- [ ] Declare `INDEX_BUCKET_COUNT = 8`. Create one main-table bucket and an ASYNC secondary index
  with exactly eight buckets; cluster replication factor remains three.
- [ ] Read the source bucket assignment and derive four disjoint roles:
  - `sourceLeader`: current source leader;
  - `recoveryFollower`: one source follower to restart and promote;
  - `offlineFollower`: the other source follower;
  - `targetOnlyServer`: the TabletServer absent from the source assignment.
- [ ] Select an Index Table bucket currently led by `targetOnlyServer` and find two deterministic
  index values hashing to it. Fail fixture setup if this topology cannot be built.

This role split is essential: both source followers can be stopped without stopping the target,
and later source leader loss cannot also remove the gated target leader.

### 7.2 Establish and persist the exact conservative prefix

- [ ] Stop both source followers and wait for ISR shrink.
- [ ] Write a first prefix on the source leader, wait for every exact physical Index Table key, and
  wait until:

```java
sourceReplica.getAllIndexPushedOffset() == sourceReplica.getLocalLogEndOffset()
```

- [ ] Trigger one source KV snapshot and assert:

```java
assertThat(snapshot.getIndexPushedOffset()).isEqualTo(baselinePushedOffset);
```

- [ ] Close the current `IndexReplicator` synchronously and unregister its source bucket from the
  existing `IndexReplicatorPool`. This is test-side use of existing lifecycle APIs; add no
  production pause hook. `close()` linearizes with any current poll, so no later source work can be
  derived by the old run.

### 7.3 Create committed but unpushed remote-only source WAL

- [ ] Write a second prefix while the old replicator is closed. Include updates from the first
  index value to the second value so recovery must both delete stale old keys and insert new keys.
- [ ] Wait for source HW to equal local end, record `committedSourceEnd`, and strongly assert
  `getAllIndexPushedOffset()` remains the persisted baseline.
- [ ] Wait until the source leader's raw remote log covers the baseline and the committed range.
- [ ] Restart only `recoveryFollower`, wait for ISR expansion, then assert its
  `localLogStartOffset()` is strictly greater than `baselinePushedOffset`, following the proven
  `KvRecoverFromRemoteLogITCase` condition.

### 7.4 Gate the replay ACK and prove the remote read before progress

- [ ] Install one scoped target hook with `ReplicaTestHooks.installAfterPutAdmissionHook(...)` on
  the selected target leader. Match the canonical source `WriterKey` and sequences greater than the
  baseline. Count down `replayAdmitted`, then wait on `releaseReplay`.
- [ ] Record the recovery follower TabletServer's
  `indexSourceRemoteReadBytes().getCount()` before leader failover.
- [ ] Stop `sourceLeader`. Because `offlineFollower` remains down, `recoveryFollower` is the only
  eligible source leader. Wait for that exact server to lead and for its new replicator to exist.
- [ ] Wait for `replayAdmitted`. While the target ACK is held, assert all of the following:

```java
assertThat(newSourceReplica.getLogTablet().localLogStartOffset())
        .isGreaterThan(baselinePushedOffset);
assertThat(newSourceReplica.getAllIndexPushedOffset())
        .isEqualTo(baselinePushedOffset);
assertThat(recoveryMetrics.indexSourceRemoteReadBytes().getCount())
        .isGreaterThan(remoteBytesBefore);
```

These three assertions prove exact snapshot restore, an unavailable local source range, and actual
raw remote-WAL consumption before the target can advance progress.

### 7.5 Release, verify exact state, and continue locally

- [ ] Release the target gate and wait for
  `newSourceReplica.getAllIndexPushedOffset() == committedSourceEnd`.
- [ ] Add `assertExactIndexProjection(Map<Integer, String> expectedRows)`, adapting the proven
  physical-KV assertion in
  `IndexPushOrderingITCase#assertIndexEqualsCommittedSourceWal`: encode each expected Index Table
  row with `CompactedKeyEncoder`, `RowEncoder`, and
  `ValueEncoder.encodeValue((short) indexTableInfo.getSchemaId(), row)`; then flush and iterate
  RocksDB on every Index Table bucket and compare the complete `Map<TableBucket, Map<String,
  String>>` of Base64 key/value bytes to the expected map. Do not hard-code schema id `1`, and do
  not settle for non-null lookup values or row count.
- [ ] Call that helper with the exact post-update source state. This single equality must prove the
  exact value bytes, absence of all stale old keys, and absence of any unexpected physical key.
- [ ] Separately lookup one known old key and one never-written key and assert both are absent, so a
  future failure is localizable rather than reported only as a whole-map diff.
- [ ] Write one post-catch-up source row to the new leader, record its new local end, and assert its
  exact pushed-offset advancement. Add that row to `expectedRows` and rerun the complete physical
  projection equality. This proves the reader hands off from remote to local WAL and keeps running
  without corrupting earlier replayed state.
- [ ] In `finally`, release and close the target hook, restart every stopped server, and assert four
  healthy TabletServers. Aggregate cleanup failures without masking the primary assertion.
- [ ] Track stopped server ids in a set and adapt `cleanup(...)` / `reportCleanupFailures(...)`
  from `IndexPushOrderingITCase`; never call `startTabletServer` for a server that this test did not
  successfully stop.
- [ ] Use only latches and `waitUntil`; add no sleep.

### 7.6 Run repeatedly and commit

- [ ] Run:

```bash
for run in 1 2 3; do
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
    -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
    -Dtest=IndexSourceRemoteRecoveryITCase test || exit 1
done
```

- [ ] Run the neighboring recovery/fencing suites:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexSourceRemoteRecoveryITCase,KvRecoverFromRemoteLogITCase,IndexPushOrderingITCase,IndexTargetRecoveryITCase test
```

- [ ] Run `git diff --check` and confirm no `Thread.sleep` in the new class.
- [ ] Commit:

```bash
git add fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceRemoteRecoveryITCase.java
git commit -m "Prove remote index source recovery"
```

---

## Task 8: Align FIP, Remove Dead Claims, And Verify The Branch

**Files:**

- Modify external document:
  `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md`
- Modify tracked design status if implementation is complete:
  `docs/superpowers/specs/2026-07-14-secondary-index-v2-production-hardening-design.md`

**Repository note:** `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip` is not a
Git repository and the FIP is not inside `fluss-index-v2`. It must be revised and verified, but it
cannot honestly be included in a Fluss Git commit. Report that external file explicitly at the end
of execution.

**Interfaces:**

- Consumes: the final code/config/metric contracts produced by Tasks 1-7.
- Produces: FIP text matching those contracts and recorded focused/workspace verification evidence.

### 8.1 Update the FIP to the implemented contract

- [ ] In §2.5.4-2.5.5, describe `IndexAccumulator` as retaining both a per-replicator threshold and
  a TabletServer total admission bound. Define pending bytes as queued + in-flight + retry-retained
  page payloads.
- [ ] In §2.6.4, replace the absolute ordinary/internal caller claim with precise protocol and trust
  boundaries:
  - the upgraded public `Table.newUpsert()` rejects `INDEX_TABLE` metadata;
  - table protocol/API magic mismatch still fails before row decode for correctness;
  - a handcrafted valid V1 request bypassing the public API is outside the trusted-client contract;
  - ApiVersion is not caller identity.
- [ ] In §2.6.8-2.6.9, state that target leader migration is indefinitely retryable and transparent
  to both SYNC and ASYNC replication. Update fault isolation to say the per-owner threshold is normal
  isolation, while total saturation may temporarily pause unrelated owners.
- [ ] In §2.6.10 and §3.2, add:

```text
index.replication.max-total-pending-bytes = 256mb
```

  and distinguish it from `index.replication.max-pending-bytes = 64mb`.
- [ ] In internal metadata descriptions, replace “only accepts internal caller” wording with
  “V1_FENCED protocol selected by system-managed Index Table metadata; public upgraded clients do
  not expose a writer.”
- [ ] In §3.4 metrics:
  - remove the nonexistent `indexPushedOffsetLag` row;
  - define `indexPushPendingBytes` using retained page bytes;
  - add `indexReplicationFailedSourceBucketCount` as the current local failed leader-source count;
  - do not add bucket labels or terminal-failure rate.
- [ ] In upgrade guidance, preserve API v2 capability sequencing but explicitly state it is not the
  public-write authorization mechanism.

### 8.2 Audit textual and code consistency

- [ ] Run these searches and inspect every hit:

```bash
rg -n "indexPushedOffsetLag|ordinary PutKv cannot write|只接受 internal|ApiVersion.*身份|pending-byte" \
  /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md \
  docs/superpowers/specs/2026-07-14-secondary-index-v2-production-hardening-design.md \
  fluss-common/src/main fluss-client/src/main fluss-server/src/main
rg -n "index.replication.max-total-pending-bytes|indexReplicationFailedSourceBucketCount" \
  /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md \
  fluss-common/src/main fluss-server/src/main fluss-server/src/test
```

Expected: stale claims are absent; new option/gauge appear in code, tests, and FIP.

- [ ] Audit all index tests for fixed sleeps:

```bash
rg -n "Thread\.sleep|TimeUnit\.[A-Z]+\.sleep" \
  fluss-server/src/test/java/org/apache/fluss/server/index \
  fluss-client/src/test/java/org/apache/fluss/client/table/FlussTableSecondaryIndexITCase.java
```

Expected: no output. Do not replace useful condition waits merely to reduce line count.

### 8.3 Run focused regression suites

- [ ] Run client coverage:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=FlussTableSecondaryIndexITCase test
```

- [ ] Run the complete index unit/integration family plus adjacent recovery:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='Index*Test,Index*ITCase,KvRecoverFromRemoteLogITCase' test
```

- [ ] If offline mode reports only missing artifacts, rerun the same command once without `-o` to
  populate `.cache`, then restore offline verification.

### 8.4 Run workspace acceptance commands

- [ ] Run exactly the project compile standard:

```bash
mvn clean compile -o -Dmaven.repo.local=.cache \
  -pl '!fluss-lake/fluss-lake-lance,!fluss-dist'
```

- [ ] Run exactly the project test standard:

```bash
mvn test -o -Dmaven.repo.local=.cache \
  -pl '!fluss-lake/fluss-lake-lance,!fluss-dist'
```

- [ ] If the local JDK cannot run Spotless, repeat only with
  `-Dspotless.check.skip=true` and report that limitation; do not describe Spotless as passing.

### 8.5 Final code and history audit

- [ ] Run:

```bash
git diff --check
git status --short
git log --oneline --decorate -10
```

- [ ] Update the tracked design document status from awaiting implementation to implemented only
  after every required test passes. Include exact test commands/results, without speculative
  production-readiness claims.
- [ ] If that tracked status changes, commit it separately:

```bash
git add -f docs/superpowers/specs/2026-07-14-secondary-index-v2-production-hardening-design.md
git commit -m "Record index hardening verification"
```

- [ ] Confirm no uncommitted code/test changes remain. Report the external FIP path separately
  because it cannot appear in Fluss Git status.

## Final Review Gate

Do not call this implementation complete unless all statements below have direct evidence:

- [ ] A real derived Index Table rejects public `newUpsert()` before any writer/RPC is created.
- [ ] A terminal failure reports once, survives as controller `FAILED`, and a stale old callback
  cannot poison a replacement.
- [ ] The server-level failed-source gauge has constant cardinality and clears on lifecycle detach.
- [ ] Pending accounting uses retained payload pages in production.
- [ ] Concurrent whole-window admissions never exceed total capacity and never partially publish.
- [ ] Retry is accounting-neutral; every ACK/failure/close path releases once.
- [ ] An impossible window terminates once instead of rereading forever.
- [ ] Target failover test stops the leader of the exact bucket addressed by its test key and writes
  during failover.
- [ ] Source failover test proves local range absence, remote byte increase, exact restored offset,
  exact final index state, stale-key deletion, and local-WAL continuation.
- [ ] FIP matches code on trust boundary, retry parity, failure health, capacity semantics, option
  names, and metric names.
- [ ] No fixed sleep or dead `indexPushedOffsetLag` remains in the reviewed scope.
- [ ] Focused and workspace-wide verification commands have recorded outcomes.
