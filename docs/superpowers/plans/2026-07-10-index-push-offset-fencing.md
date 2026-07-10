# Offset-Fenced Index Push Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-row index versions and logical tombstones with a stable-writer, source-WAL-offset fence that makes physical Index Table UPSERT and DELETE safe across source and target failover.

**Architecture:** Every source `TableBucket` receives a persistent writer ID. Index batches carry that writer ID, the exclusive source WAL end offset, and the source partition ID in a new internal KV batch format. Target IndexBuckets reuse generic WAL writer-state and delayed-HW-ack machinery with a separate `MONOTONIC_SOURCE_OFFSET` policy, while partition IDs in physical keys isolate partition incarnations.

**Tech Stack:** Java, Maven, Fluss KV/WAL record formats, ZooKeeper metadata, Fluss RPC/protogen, RocksDB-backed KV tablets, JUnit 5, AssertJ, JMH.

## Global Constraints

- Source ordering is the exclusive source `TableBucket` WAL end offset stored as `long`.
- Replication remains at-least-once and eventually consistent; exactly-once is not required.
- Ordinary client writers retain the existing `int` contiguous sequence contract.
- Internal ordering state lives in generic target WAL writer-state snapshots, not an Index Table-specific store or RocksDB column family.
- A stale internal batch must not decode rows, mutate KV, or append target WAL.
- A stale acknowledgement waits for the target WAL offset that dominates the stored frontier to enter HW.
- Active internal writer state never expires by ordinary writer TTL.
- Partitioned physical keys are `(indexColumns, basePrimaryKey, __partition_id)` and values continue carrying `__partition_id`.
- Existing partition tombstone write, query, and compaction filters remain.
- `__source_offset`, `__index_deleted`, versioned Index Table merge configuration, and logical index visibility filtering are removed.
- Generic `VersionedRowMerger` support outside secondary indexes remains.
- The rejected Index Table format never entered Git history; do not build migration machinery for it.
- Unknown internal magic, unsupported tables, and incomplete tombstone baseline states fail before KV prewrite.
- Preserve unrelated dirty-worktree changes; never reset or rewrite user changes.
- Asynchronous tests use latches, fault hooks, or condition waiting. Do not add fixed `Thread.sleep` synchronization.

---

## File And Interface Map

- `TableAssignment` and `PartitionAssignment` own persistent `indexWriterIdBase`.
- `CoordinatorContext` derives `writerId = base + bucketId`; NotifyLeaderAndIsr carries it to `Replica`.
- `KvRecordBatch` and `LogRecordBatch` own the request and persisted internal formats.
- `WriterStateManager` owns validation, recovery, expiry, retirement, and snapshots.
- `LogTablet` owns frontier serialization, stale result offsets, and snapshot-before-delete.
- `KvTablet` owns the no-prewrite stale path and ambiguous-append fail-stop decision.
- `IndexTableDescriptorFactory` and `IndexSpecFactory` own physical schema and pid-qualified encoding.
- `IndexReplicator` owns physical mutations and internal batch metadata.
- `ReplicaIndexController` owns tombstone readiness and the internal write guard.

---

## Task 0: Freeze the rejected-design baseline without committing it

**Artifacts outside the repository:**
- Create directory: `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/.index-v2-rejected-baseline-20260710/`
- Create: `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/.index-v2-rejected-baseline-20260710.manifest`

- [ ] **Step 1: Inventory the current dirty worktree**

```bash
git status --short
git diff --stat
git diff --check
```

Expected: every pre-existing modified and untracked path is understood. Do not reset, stash, or commit the rejected per-row format merely to obtain a checkpoint.

- [ ] **Step 2: Copy the exact baseline outside the repository**

```bash
BASELINE_DIR=/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/.index-v2-rejected-baseline-20260710
test ! -e "$BASELINE_DIR"
rsync -a --exclude=.git --exclude=.cache --exclude=target \
  /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-index-v2/ \
  "$BASELINE_DIR"/
```

This copy includes tracked modifications and untracked source files but excludes generated build output. It is read-only reference input for Task 9, not a supported migration artifact.

- [ ] **Step 3: Record a reproducible manifest**

```bash
BASELINE_DIR=/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/.index-v2-rejected-baseline-20260710
find "$BASELINE_DIR" -type f -exec shasum -a 256 {} + | LC_ALL=C sort \
  > /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/.index-v2-rejected-baseline-20260710.manifest
shasum -a 256 /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/.index-v2-rejected-baseline-20260710.manifest
```

Record the manifest checksum in the execution notes. Task 9 must benchmark this frozen copy, not reconstruct the rejected design from memory.

- [ ] **Step 4: Run the existing focused tests in the copied baseline**

```bash
mvn -o -Dmaven.repo.local=/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-index-v2/.cache \
  -f /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/.index-v2-rejected-baseline-20260710/pom.xml \
  -pl fluss-server -am -Dspotless.check.skip=true \
  -Dtest=IndexReplicatorAppendTest,IndexPushReplicationITCase,IndexPushFailoverITCase test
```

Expected: the frozen baseline compiles and its existing index tests pass. Preserve failures as baseline facts; do not edit the copy to make an unexplained failure disappear.

---

## Task 1: Persist And Propagate Stable Source Writer IDs

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/ZooKeeperClient.java:1425-1432`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/TableAssignment.java:30-102`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/PartitionAssignment.java:30-72`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/TableAssignmentJsonSerde.java:35-90`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/PartitionAssignmentJsonSerde.java:35-66`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/MetadataManager.java:375-430,808-900`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorContext.java:340-395`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/TableManager.java:90-150`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorRequestBatch.java:214-246`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessor.java:1665-1710`
- Modify: `fluss-rpc/src/main/proto/FlussApi.proto:1004-1016`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/entity/NotifyLeaderAndIsrData.java:28-100`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java:752-805`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java:205-215,455-565`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/zk/data/TableAssignmentJsonSerdeTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/zk/data/PartitionAssignmentJsonSerdeTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/zk/ZooKeeperClientTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/coordinator/CoordinatorContextTest.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/utils/ServerRpcMessageUtilsIndexWriterTest.java`

**Interfaces:**
- Produces: `OptionalLong TableAssignment.getIndexWriterIdBase()`
- Produces: `TableAssignment TableAssignment.withIndexWriterIdBase(long base)`
- Produces: `long ZooKeeperClient.reserveWriterIds(long count)`
- Produces: `OptionalLong CoordinatorContext.getIndexWriterId(TableBucket tableBucket)`
- Produces: `OptionalLong NotifyLeaderAndIsrData.getIndexWriterId()`
- Produces: `long Replica.getIndexWriterId()` using `NO_WRITER_ID` when absent

- [ ] **Step 1: Add failing assignment serde tests**

```java
@Test
void testIndexWriterIdBaseRoundTripAndV1Compatibility() {
    TableAssignment v2 =
            TableAssignment.builder()
                    .add(0, BucketAssignment.of(1, 2))
                    .add(3, BucketAssignment.of(2, 3))
                    .indexWriterIdBase(100L)
                    .build();
    assertThat(roundTrip(v2).getIndexWriterIdBase()).hasValue(100L);
    assertThat(readJson("{\"version\":1,\"buckets\":{\"0\":[1,2]}}")
                    .getIndexWriterIdBase())
            .isEmpty();
}

@Test
void testPartitionAssignmentPreservesWriterBase() {
    PartitionAssignment assignment = new PartitionAssignment(7L, assignments(), 1000L);
    assertThat(roundTrip(assignment).getIndexWriterIdBase()).hasValue(1000L);
}
```

- [ ] **Step 2: Run the serde tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=TableAssignmentJsonSerdeTest,PartitionAssignmentJsonSerdeTest test
```

Expected: compilation fails because the base field, builder method, and partition constructor do not exist.

- [ ] **Step 3: Implement versioned assignment metadata**

```java
private final @Nullable Long indexWriterIdBase;

public OptionalLong getIndexWriterIdBase() {
    return indexWriterIdBase == null
            ? OptionalLong.empty()
            : OptionalLong.of(indexWriterIdBase);
}

public TableAssignment withIndexWriterIdBase(long base) {
    checkArgument(base >= 0L, "indexWriterIdBase must be non-negative");
    return new TableAssignment(assignments, base);
}
```

Serialize `index_writer_id_base` only when present, deserialize it optionally, and bump both assignment JSON versions to `2`. Preserve old constructors by delegating with `null`.

- [ ] **Step 4: Add failing range and propagation tests**

```java
@Test
void testReserveWriterIdRangeIsDisjoint() throws Exception {
    long first = zookeeperClient.reserveWriterIds(4L);
    long second = zookeeperClient.reserveWriterIds(3L);
    assertThat(second).isEqualTo(first + 4L);
}

@Test
void testCoordinatorContextDerivesBucketWriterId() {
    context.putIndexWriterIdBase(7L, null, 100L);
    assertThat(context.getIndexWriterId(new TableBucket(7L, 3))).hasValue(103L);
}

@Test
void testNotifyLeaderRoundTripCarriesWriterId() {
    NotifyLeaderAndIsrData decoded = roundTrip(notifyDataWithWriterId(1234L));
    assertThat(decoded.getIndexWriterId()).hasValue(1234L);
}
```

Run the range assertion concurrently with varied spans, sort returned intervals, and strongly assert every adjacent pair is disjoint. Inject assignment persistence failure after reservation and assert the next reservation starts beyond the leaked range rather than reusing it.

- [ ] **Step 5: Implement atomic range allocation**

```java
public long reserveWriterIds(long count) throws Exception {
    checkArgument(count > 0L, "writer id range size must be positive");
    long base = writerIdCounter.getAndAdd(count);
    checkState(base >= 0L && base <= Long.MAX_VALUE - count,
            "writer id range overflows: base=%s, count=%s", base, count);
    return base;
}
```

Before persisting each data-table or data-partition assignment, reserve `span = maxBucketId + 1`. Reject negative bucket IDs. An allocation may leak if later creation fails, but it is never reclaimed. Derived Index Tables do not receive a source range.

- [ ] **Step 6: Preserve bases in Coordinator state and reassignment**

Populate table/partition base maps from `TableManager`, clear them on completed deletion, and preserve the original base in reassignment writes.

```java
public OptionalLong getIndexWriterId(TableBucket bucket) {
    OptionalLong base = getIndexWriterIdBase(bucket.getTableId(), bucket.getPartitionId());
    return base.isPresent()
            ? OptionalLong.of(Math.addExact(base.getAsLong(), bucket.getBucket()))
            : OptionalLong.empty();
}
```

- [ ] **Step 7: Propagate the ID through NotifyLeaderAndIsr**

```protobuf
optional int64 index_writer_id = 9;
```

`CoordinatorRequestBatch` resolves it from `CoordinatorContext`; RPC conversion preserves field presence; `Replica.makeLeader/makeFollower` stores it. An indexed source replica with a missing ID remains deferred instead of sending unordered batches.

- [ ] **Step 8: Run Task 1 tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=TableAssignmentJsonSerdeTest,PartitionAssignmentJsonSerdeTest,ZooKeeperClientTest,CoordinatorContextTest,ServerRpcMessageUtilsIndexWriterTest test
```

Expected: old v1 JSON remains readable; ranges are disjoint; reassignment preserves bases; RPC presence and absence round trip exactly.

- [ ] **Step 9: Commit Task 1**

```bash
git add fluss-rpc/src/main/proto/FlussApi.proto \
  fluss-server/src/main/java/org/apache/fluss/server/zk/ZooKeeperClient.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/TableAssignment.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/PartitionAssignment.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/TableAssignmentJsonSerde.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/PartitionAssignmentJsonSerde.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/MetadataManager.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorContext.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/TableManager.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorRequestBatch.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessor.java \
  fluss-server/src/main/java/org/apache/fluss/server/entity/NotifyLeaderAndIsrData.java \
  fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/data/TableAssignmentJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/data/PartitionAssignmentJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/ZooKeeperClientTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/CoordinatorContextTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/utils/ServerRpcMessageUtilsIndexWriterTest.java
git commit -m "[index-v2] persist index writer identity"
```

---

## Task 2: Add Isolated Internal KV And WAL Batch Formats

**Files:**
- Create: `fluss-common/src/main/java/org/apache/fluss/record/WriterStateMode.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatch.java:30-100`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecordBatch.java:35-190`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatchBuilder.java:45-220`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/LogRecordBatch.java:95-150`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/LogRecordBatchFormat.java:30-390`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/DefaultLogRecordBatch.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsCompactedBuilder.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsArrowBuilder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/wal/WalBuilder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/wal/CompactedWalBuilder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/wal/ArrowWalBuilder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/wal/IndexWalBuilder.java`
- Test: `fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordBatchTest.java`
- Test: `fluss-common/src/test/java/org/apache/fluss/record/MemoryLogRecordsCompactedBuilderTest.java`
- Test: `fluss-common/src/test/java/org/apache/fluss/record/MemoryLogRecordsArrowBuilderTest.java`

**Interfaces:**
- Produces: `WriterStateMode { NONE, CONTIGUOUS, MONOTONIC_SOURCE_OFFSET }`
- Produces: `long writerSequence()`, `long sourcePartitionId()`, and `WriterStateMode writerStateMode()` on KV and WAL batches
- Produces: `setInternalWriterState(long writerId, long sourceEndOffset, long sourcePartitionId)` on KV and WAL builders

- [ ] **Step 1: Write failing KV format tests**

```java
@Test
void testInternalWriterMetadataRoundTrip() throws Exception {
    KvRecordBatchBuilder builder = internalBuilder();
    builder.setInternalWriterState(11L, ((long) Integer.MAX_VALUE) + 99L, 23L);
    builder.append(KEY, VALUE);
    KvRecordBatch batch = pointTo(builder.build());
    assertThat(batch.writerStateMode()).isEqualTo(MONOTONIC_SOURCE_OFFSET);
    assertThat(batch.writerId()).isEqualTo(11L);
    assertThat(batch.writerSequence()).isEqualTo(((long) Integer.MAX_VALUE) + 99L);
    assertThat(batch.sourcePartitionId()).isEqualTo(23L);
    assertThat(batch.isValid()).isTrue();
}

@Test
void testOrdinaryBatchRemainsContiguous() {
    assertThat(ordinaryBatch.writerStateMode()).isEqualTo(CONTIGUOUS);
    assertThat(ordinaryBatch.writerSequence()).isEqualTo(ordinaryBatch.batchSequence());
}
```

Also assert internal writer IDs and source end offsets are non-negative, and `sourcePartitionId` is either non-negative or the single explicit `NO_PARTITION_ID` marker. Malformed combinations and truncated magic-v1 headers must fail CRC/header validation before record iteration.

- [ ] **Step 2: Run KV tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common \
  -Dspotless.check.skip=true -Dtest=DefaultKvRecordBatchTest test
```

Expected: compilation fails on the new mode, builder, and accessors.

- [ ] **Step 3: Implement KV magic v1 without changing public defaults**

```java
public enum WriterStateMode {
    NONE,
    CONTIGUOUS,
    MONOTONIC_SOURCE_OFFSET
}
```

Keep public writers on KV magic v0. Internal magic v1 replaces the v0 `int32 sequence` with `int64 sourceEndOffset` and appends `int64 sourcePartitionId`. Make header offsets and sizes magic-dependent; CRC still covers schema ID through records. Reject ordinary state setters on v1 and internal setters on v0. Legacy `batchSequence()` access on internal magic must fail fast rather than truncate the long value; generic validation uses `writerSequence()`.

- [ ] **Step 4: Write failing target WAL format tests**

```java
@Test
void testInternalWriterMetadataSurvivesWalBuild() throws Exception {
    CompactedWalBuilder builder = newCompactedWalBuilder();
    builder.setInternalWriterState(11L, 5_000_000_000L, 23L);
    builder.append(INSERT, KEY, VALUE);
    LogRecordBatch batch = onlyBatch(builder.build());
    assertThat(batch.writerStateMode()).isEqualTo(MONOTONIC_SOURCE_OFFSET);
    assertThat(batch.writerSequence()).isEqualTo(5_000_000_000L);
    assertThat(batch.sourcePartitionId()).isEqualTo(23L);
}
```

- [ ] **Step 5: Implement WAL magic v3**

Keep ordinary builders on current magic. Add log magic v3 with `int64 writerSequence` and `int64 sourcePartitionId`. Existing v0-v2 batches with writer metadata report `CONTIGUOUS` and widen the current `int` sequence; batches without writer metadata report `NONE`:

```java
default long writerSequence() {
    return batchSequence();
}

default long sourcePartitionId() {
    return NO_PARTITION_ID;
}
```

All WAL builders select v3 only after `setInternalWriterState`. Follower parsing and file projection recognize v3 header and CRC offsets.

- [ ] **Step 6: Run Task 2 tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=DefaultKvRecordBatchTest,MemoryLogRecordsCompactedBuilderTest,MemoryLogRecordsArrowBuilderTest test
```

Expected: v0-v2 compatibility and v1/v3 internal round trips pass; malformed magic and short headers are rejected.

- [ ] **Step 7: Commit Task 2**

```bash
git add fluss-common/src/main/java/org/apache/fluss/record \
  fluss-common/src/test/java/org/apache/fluss/record \
  fluss-server/src/main/java/org/apache/fluss/server/kv/wal
git commit -m "[index-v2] add internal writer batch format"
```

---

## Task 3: Implement Monotonic Writer State And Durable Frontiers

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/WriterStateEntry.java:30-160`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/WriterAppendInfo.java:30-175`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/WriterStateManager.java:75-540`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogAppendInfo.java:20-145`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java:700-820,1100-1380`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/log/WriterStateManagerTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/log/LogTabletTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogTabletTest.java`

**Interfaces:**
- Produces: `OptionalLong LogTablet.findDominatingInternalOffset(long writerId, long sourceEndOffset)`
- Produces: `boolean WriterStateManager.removeInternalWritersForPartition(long partitionId)`
- Produces: `void WriterStateManager.takeForcedSnapshot()`
- Produces: `boolean LogTablet.retireInternalWritersForPartition(long partitionId)`
- Produces: `int WriterStateManager.internalWriterCount()`
- Produces: `LogAppendInfo LogAppendInfo.staleAt(long targetWalOffset)`

- [ ] **Step 1: Write failing state-machine tests**

```java
@Test
void testMonotonicWriterAcceptsGapsAndStalesAnyOlderOffset() throws Exception {
    appendInternal(WRITER, 100L, PID);
    appendInternal(WRITER, 500L, PID);
    appendInternal(WRITER, 5_000_000_000L, PID);
    long before = logEndOffset();
    LogAppendInfo stale = appendInternal(WRITER, 100L, PID);
    assertThat(stale.duplicated()).isTrue();
    assertThat(stale.lastOffset()).isEqualTo(offsetOf(5_000_000_000L));
    assertThat(logEndOffset()).isEqualTo(before);
}

@Test
void testOrdinaryWriterStillRejectsGap() {
    appendOrdinary(WRITER, 0);
    assertThatThrownBy(() -> appendOrdinary(WRITER, 2))
            .isInstanceOf(OutOfOrderSequenceException.class);
}
```

- [ ] **Step 2: Run state tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=WriterStateManagerTest,LogTabletTest test
```

Expected: internal long monotonic state is not represented.

- [ ] **Step 3: Extend WriterStateEntry without weakening contiguous writers**

Keep the five-entry deque for `CONTIGUOUS`; store one frontier for `MONOTONIC_SOURCE_OFFSET`:

```java
static final class InternalBatchMetadata {
    final long sourceEndOffset;
    final long targetWalOffset;
    final long timestamp;
    final long sourcePartitionId;
}
```

`WriterAppendInfo` accepts any increasing internal sequence and reports lower/equal as stale. A writer ID cannot change mode.

- [ ] **Step 4: Implement stale revalidation inside LogTablet**

`findDominatingInternalOffset` runs under the `LogTablet` lock. `analyzeAndValidateWriterState` revalidates at append time. `E <= last` returns `LogAppendInfo.staleAt(lastInternal.targetWalOffset)` without append; `E > last` appends and advances state.

- [ ] **Step 5: Write failing durability tests**

```java
@Test
void testInternalWriterDoesNotExpire() {
    loadInternalWriter(WRITER, 900L, PID, now);
    manager.removeExpiredWriters(now + writerExpirationMs + 1L);
    assertThat(manager.lastEntry(WRITER)).isPresent();
}

@Test
void testSegmentDeletionCannotDeleteLastCoveringFrontier() throws Exception {
    appendInternalAndRoll(WRITER, 900L, PID);
    deleteOldSegmentsAndRestart();
    assertThat(appendInternal(WRITER, 800L, PID).duplicated()).isTrue();
}
```

Add two complementary unknown-writer recovery tests: with `logStartOffset == 0`, full WAL scan proves history completeness and a new writer may start at any non-negative offset; with `logStartOffset > 0`, missing/corrupt covering state fails tablet recovery instead of treating the writer as new. A validated full snapshot that omits a never-seen writer is sufficient proof to accept that writer's first batch.

- [ ] **Step 6: Version snapshots and protect frontier retention**

Writer snapshot v2 persists `mode`, `last_writer_sequence`, `last_target_wal_offset`, and `source_partition_id`; v1 reads as contiguous. Internal entries bypass TTL. Serialize entries in writer-ID order and protect the v2 payload with a checksum so structurally valid but corrupted JSON cannot establish a false frontier.

Before deleting WAL that can reconstruct an internal entry, write a full snapshot to a temporary file, fsync it, atomically move it with parent-directory flush, read back and validate checksum/content, verify its offset dominates the last deleted segment end, then delete old WAL and older snapshots. Retain at least one validated covering snapshot. Mark writer history complete only after scanning from `logStartOffset == 0` or loading a validated full snapshot that covers retained history and replaying its WAL tail. Remove the clean-shutdown branch that manufactures empty writer snapshots merely because no snapshot exists; absence now triggers WAL scan, and missing state with truncated history fails recovery before the tablet becomes online.

- [ ] **Step 7: Implement partition retirement primitive**

```java
public boolean removeInternalWritersForPartition(long partitionId) {
    List<Long> ids = writers.entrySet().stream()
            .filter(e -> e.getValue().isInternal()
                    && e.getValue().sourcePartitionId() == partitionId)
            .map(Map.Entry::getKey)
            .collect(toList());
    removeWriterIds(ids);
    return !ids.isEmpty();
}
```

Return whether the map changed and add `takeForcedSnapshot()` for metadata-driven retirement. Unlike ordinary offset-triggered snapshots, it atomically replaces the snapshot at the current map-end offset even when no new WAL record advanced that offset. Keep these manager methods package-private; `LogTablet.retireInternalWritersForPartition` invokes them while holding the log lock.

- [ ] **Step 8: Run Task 3 tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=WriterStateManagerTest,LogTabletTest,RemoteLogTabletTest test
```

Expected: sparse offsets, arbitrary stale success, ordinary gap rejection, TTL immunity, restart, truncation, segment deletion, and remote recovery pass.

- [ ] **Step 9: Commit Task 3**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/log \
  fluss-server/src/test/java/org/apache/fluss/server/log
git commit -m "[index-v2] add monotonic writer frontier"
```

## Task 4: Add the target stale fast path and typed fail-stop append outcomes

**Files:**
- Create: `fluss-server/src/main/java/org/apache/fluss/server/log/AmbiguousLogAppendException.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogSegment.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LocalLog.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogAppendInfo.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogAppendInfo.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/log/LogSegmentTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/log/LogTabletTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java`

- [ ] **Step 1: Write a failing stale fast-path test**

Append internal source offset `100`, advance target HW past its target WAL offset, then submit offset `90`. Instrument the KV prewrite path and assert that the stale request performs no decode, no prewrite, no target WAL append, and returns the dominating target WAL offset.

```java
assertThat(result.stale()).isTrue();
assertThat(result.lastOffset()).isEqualTo(offsetForSource100);
verify(kvPreWriteBuffer, never()).preWrite(any());
assertThat(logTablet.localLogEndOffset()).isEqualTo(endBeforeStale);
```

- [ ] **Step 2: Run the focused test to verify the red state**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=KvTabletTest#testStaleInternalBatchSkipsPrewrite test
```

Expected: the current implementation enters the prewrite path.

- [ ] **Step 3: Implement the early stale check and append-time revalidation**

Before decoding records or touching the prewrite buffer, ask `LogTablet` for a dominating internal frontier:

```java
if (records.writerStateMode() == WriterStateMode.MONOTONIC_SOURCE_OFFSET) {
    OptionalLong dominatingOffset =
            logTablet.findDominatingInternalOffset(
                    records.writerId(), records.sourceEndOffset());
    if (dominatingOffset.isPresent()) {
        return LogAppendInfo.staleAt(dominatingOffset.getAsLong());
    }
}
```

Treat this only as a fast path. `LogTablet.append` must repeat the comparison while holding its writer-state lock, so two concurrent requests cannot both pass validation and append out of order.

- [ ] **Step 4: Write failing ambiguous-append tests**

Inject failures before physical WAL write, after bytes are appended but before offset-index completion, and after `localLogEndOffset` advances:

```java
assertThatThrownBy(this::failBeforeWalAppend)
        .isInstanceOf(ExpectedRetryableException.class);
verify(kvPreWriteBuffer).truncateTo(mark);

assertThatThrownBy(this::failAfterWalBytesAppend)
        .isInstanceOf(AmbiguousLogAppendException.class);
verify(fatalErrorHandler).onFatalError(any(AmbiguousLogAppendException.class));
```

Also inject a partial `FileLogRecords.append` failure and assert it is ambiguous even when in-memory LEO is unchanged. Assert that no post-write path is converted into an ordinary retry on the same live replica.

- [ ] **Step 5: Make the log layer report an explicit append outcome**

`LogSegment.append` records the active log-file position before write. If `FileLogRecords.append` or later index maintenance throws and the file position changed, wrap the cause in `AmbiguousLogAppendException`. `LogTablet.append` also wraps any failure after `LocalLog.append` returns, including writer-state update or flush failure. A segment roll without WAL bytes is not ambiguous.

```java
int beforeBytes = fileLogRecords.sizeInBytes();
try {
    int physicalPosition = beforeBytes;
    fileLogRecords.append(records);
    if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        offsetIndex().append(largestOffset, physicalPosition);
        timeIndex().maybeAppend(maxTimestampSoFar(), startOffsetOfMaxTimestampSoFar());
    }
} catch (Throwable failure) {
    if (fileLogRecords.sizeInBytes() != beforeBytes) {
        throw new AmbiguousLogAppendException(tableBucket, failure);
    }
    throw failure;
}
```

The existing `KvTablet.kvLock` serializes this decision with all other tablet writes. `KvTablet` propagates `AmbiguousLogAppendException` without truncating prewrite state; it truncates only for failures the log layer proves occurred before WAL bytes. `Replica.putRecordsToLeader` routes the typed exception to the existing fatal-error handler. Document that recovery must rebuild KV and writer state from valid target WAL before this replica serves writes again.

- [ ] **Step 6: Prove stale success waits for the dominating HW**

Hold the target WAL high watermark below the offset that established source offset `100`; submit stale offset `90`; assert its future remains incomplete. Advance HW to the dominating target offset and assert success. Repeat with a leader transition before HW advancement and assert the caller retries against the recovered leader.

- [ ] **Step 7: Run Task 4 tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=LogSegmentTest,LogTabletTest,KvTabletTest,ReplicaTest test
```

Expected: stale batches bypass prewrite, append-time races are fenced, ambiguous appends fail-stop, and acknowledgements remain HW-bound.

- [ ] **Step 8: Commit Task 4**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/log \
  fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/test/java/org/apache/fluss/server/log \
  fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java
git commit -m "[index-v2] fence stale target writes"
```

## Task 5: Emit physical index mutations with partition-aware keys

**Files:**
- Modify: `fluss-common/src/main/java/org/apache/fluss/utils/IndexTableUtils.java`
- Test: `fluss-common/src/test/java/org/apache/fluss/utils/IndexTableUtilsTest.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexAccumulator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexTableDescriptorFactory.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexSpec.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexSpecFactory.java`
- Create: `fluss-server/src/main/java/org/apache/fluss/server/index/PartitionedIndexKeyEncoder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexWindow.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java`
- Delete: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexEntryVisibilityFilter.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexTableDescriptorFactoryTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSpecFactoryTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexAccumulatorTest.java`
- Delete: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexEntryVisibilityFilterTest.java`

- [ ] **Step 1: Write failing schema-contract tests**

For a partitioned source table, assert the physical index primary key is exactly `(index columns, base primary key, __partition_id)`. Assert the value contains the source row fields required by recheck plus `__partition_id`, and that neither `__source_offset` nor `__index_deleted` exists.

```java
assertThat(indexSchema.getPrimaryKeyColumnNames())
        .containsExactly("indexed_col", "base_pk", "__partition_id");
assertThat(indexSchema.getColumnNames())
        .doesNotContain("__source_offset", "__index_deleted");
```

- [ ] **Step 2: Run the descriptor test to verify the red state**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexTableDescriptorFactoryTest test
```

Expected: the current descriptor still exposes logical-delete/version columns or excludes the partition ID from the physical key.

- [ ] **Step 3: Implement the partition-aware key encoder**

`PartitionedIndexKeyEncoder` mirrors `CompactedKeyEncoder`, reuses `CompactedKeyWriter` through `KeyEncodingRecycler`, and appends a fixed BIGINT partition ID directly to the compacted key. Do not allocate a `GenericRow` for each index row.

```java
BinaryRow encode(RowData sourceRow, long partitionId) {
    CompactedKeyWriter writer = recycler.borrow();
    try {
        writer.writeProjectedKey(sourceRow);
        writer.writeLong(partitionId);
        return writer.finish();
    } finally {
        recycler.recycle(writer);
    }
}
```

Keep the non-partitioned encoder byte-compatible with the existing key format.

- [ ] **Step 4: Simplify IndexSpec encoding contracts**

Change the value encoder from `encode(row, sourceOffset, deleted)` to `encode(row, partitionId)` for partitioned sources and `encode(row)` otherwise. Add explicit `encodeDeleteKey(oldRow, partitionId)` and `encodeUpsertKey(newRow, partitionId)` paths so key-changing updates emit a physical DELETE for the old key and UPSERT for the new key.

- [ ] **Step 5: Write failing mutation tests**

Cover insert, same-key update, index-key-changing update, delete, partitioned key collision, and repeated replay:

```java
assertThat(mutationsFor(updateChangingIndexKey))
        .containsExactly(delete(oldPhysicalKey), upsert(newPhysicalKey, newValue));
assertThat(keyFor(row, 7L)).isNotEqualTo(keyFor(row, 8L));
assertThat(replayTwice(finalKvState)).isEqualTo(replayOnce(finalKvState));
```

- [ ] **Step 6: Emit internal writer metadata on every target batch**

Pass the stable source writer ID into `IndexReplicator`. Immediately before each `BucketBatchBuilder.build()`, set:

```java
builder.setInternalWriterState(
        sourceWriterId,
        window.lastProcessedOffset(),
        sourcePartitionId);
```

Add `BucketBatchBuilder.appendDelete(key)` as `builder.append(key, null)`. Remove the `IndexWindow` claim that replay reproduces identical window boundaries; correctness now depends only on monotonic end offsets and physical mutation order.

- [ ] **Step 7: Remove logical visibility code and dead tests**

Delete `IndexEntryVisibilityFilter`, its unit tests, versioned index-table merge configuration, and every read/write dependency on `__source_offset` or `__index_deleted`. Retain the partition tombstone query and compaction filters because they protect partition-drop semantics independently of row ordering.

- [ ] **Step 8: Run Task 5 tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexTableDescriptorFactoryTest,IndexSpecFactoryTest,IndexReplicatorAppendTest test
```

Expected: physical DELETE/UPSERT semantics, partition-aware keys, and internal batch headers pass without logical visibility fields.

- [ ] **Step 9: Commit Task 5**

```bash
git add fluss-common/src/main/java/org/apache/fluss/utils/IndexTableUtils.java \
  fluss-common/src/test/java/org/apache/fluss/utils/IndexTableUtilsTest.java \
  fluss-server/src/main/java/org/apache/fluss/server/index \
  fluss-server/src/test/java/org/apache/fluss/server/index
git commit -m "[index-v2] use physical index mutations"
```

## Task 6: Gate partition traffic on a complete tombstone baseline

**Files:**
- Modify: `fluss-rpc/src/main/proto/FlussApi.proto`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/metadata/ClusterMetadata.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/metadata/TabletServerMetadataCache.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorRequestBatch.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java`
- Create: `fluss-common/src/main/java/org/apache/fluss/exception/IndexMetadataNotReadyException.java`
- Create: `fluss-server/src/main/java/org/apache/fluss/server/index/InternalIndexWriteGuard.java`
- Modify: `fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/Errors.java`
- Test: `fluss-rpc/src/test/java/org/apache/fluss/rpc/protocol/ApiErrorTest.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/TombstonedPartitionDiscriminator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/metadata/TabletServerMetadataCacheTest.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/index/InternalIndexWriteGuardTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/metadata/PartitionTombstoneMetadataPropagationTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaManagerTest.java`

- [ ] **Step 1: Write failing metadata-baseline tests**

Apply an incremental tombstone update and assert the cache remains not ready. Apply a full snapshot, including an empty full snapshot, and assert readiness becomes true only after the whole update is installed.

```java
cache.updateMetadata(incrementalTombstoneUpdate());
assertThat(cache.hasPartitionTombstoneBaseline()).isFalse();

cache.updateMetadata(emptyFullTombstoneSnapshot());
assertThat(cache.hasPartitionTombstoneBaseline()).isTrue();
```

Also deliver a newer incremental tombstone before an older full snapshot and assert the installed state retains the greater per-table tombstone version.

- [ ] **Step 2: Add the explicit full-snapshot marker**

Add this backward-compatible protobuf field:

```proto
optional bool partition_tombstones_full_snapshot = 7 [default = false];
```

Carry the marker through `ClusterMetadata`, `CoordinatorRequestBatch`, `ServerRpcMessageUtils`, and `TabletServerMetadataCache`. Never infer completeness from a non-empty list. The startup full snapshot and any already-observed incremental entries are combined by retaining the greatest per-table tombstone version, then readiness is published after the resulting map is installed.

- [ ] **Step 3: Write failing write-guard tests**

Cover all three outcomes:

```java
assertThatThrownBy(() -> guard.validate(batchBeforeBaseline))
        .isInstanceOf(IndexMetadataNotReadyException.class);
assertThat(guard.validate(batchForTombstonedPartition)).isEqualTo(NO_OP);
assertThat(guard.validate(batchForActivePartition)).isEqualTo(APPLY);
```

For active partitioned writes, reject a header partition ID that differs from any UPSERT value tag or DELETE key partition ID. Assert rejected and tombstoned requests do not advance internal writer state or mutate KV.

- [ ] **Step 4: Implement the guard before writer-state and KV processing**

`InternalIndexWriteGuard` is invoked only for internal index-table batches. Non-partitioned batches use the explicit `NO_PARTITION_ID` marker and bypass tombstone-baseline checks. Its order for partitioned batches is:

1. Require a complete tombstone baseline.
2. If the source partition is tombstoned, return immediate metadata-backed `LogAppendInfo.noOp()` success with no required WAL offset before writer-state validation, delayed-HW acknowledgement, row decode, or prewrite.
3. Otherwise validate the batch partition ID against every physical key/value partition tag and return `APPLY`.

Make `IndexMetadataNotReadyException` extend `RetriableException`, allocate the next unassigned stable protocol error code, and add a uniqueness/round-trip assertion in the RPC error tests. Malformed partition identity remains a non-retryable corruption error.

- [ ] **Step 5: Write failing retirement, follower-lag, and drop-race tests**

Block an active partition write at the guard, install the tombstone, release it, and assert no visible index row and no retained writer frontier. With `requiredAcks=-1`, assert tombstone no-op completes immediately without registering a HW-dependent delayed write. Delay follower replication of a pre-drop target WAL until after retirement and assert follower apply removes the recreated frontier. Run retirement concurrently with segment cleanup and assert both complete without deadlock. Restart from WAL/snapshot and assert the same after baseline reconciliation. Also assert a non-partitioned internal batch proceeds before any tombstone baseline exists.

- [ ] **Step 6: Serialize writer retirement with KV and follower apply**

After installing a tombstone metadata update, `ReplicaManager` visits online index replicas and invokes retirement under the same per-tablet serialization used by KV apply:

```java
kvTablet.retireInternalWriterState(partitionId);
```

`KvTablet.retireInternalWriterState` acquires its write lock and delegates to the locked `LogTablet` API; do not expose `WriterStateManager` through KV. Follower apply already holding the reentrant KV write lock calls the same method. Preserve the established `kvLock -> LogTablet lock` acquisition order and add a concurrent segment-cleanup/retirement test that completes without deadlock. Retirement is idempotent. A concurrent old partition request either applies before the tombstone and is later hidden/compacted, or observes the tombstone and becomes a no-op; it cannot recreate retired state after the metadata barrier.

Follower apply is a distinct late path: a target WAL batch accepted before drop may arrive after local retirement. After appending/applying an internal v3 follower batch, recheck its `sourcePartitionId` under the same KV serialization; if now tombstoned, remove the reconstructed frontier immediately. The follower still retains the replicated target WAL for log correctness, while query/compaction filters keep its row effects invisible.

- [ ] **Step 7: Run Task 6 tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=ApiErrorTest,TabletServerMetadataCacheTest,InternalIndexWriteGuardTest,KvTabletTest,ReplicaManagerTest test
```

Expected: readiness, tombstoned no-op, active validation, retirement serialization, restart, and race assertions pass.

- [ ] **Step 8: Commit Task 6**

```bash
git add fluss-rpc/src/main/proto/FlussApi.proto \
  fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/Errors.java \
  fluss-rpc/src/test/java/org/apache/fluss/rpc/protocol/ApiErrorTest.java \
  fluss-common/src/main/java/org/apache/fluss/exception/IndexMetadataNotReadyException.java \
  fluss-server/src/main/java/org/apache/fluss/server/metadata/ClusterMetadata.java \
  fluss-server/src/main/java/org/apache/fluss/server/metadata/TabletServerMetadataCache.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorRequestBatch.java \
  fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/InternalIndexWriteGuard.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/TombstonedPartitionDiscriminator.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/LogAppendInfo.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java \
  fluss-server/src/test/java/org/apache/fluss/server/metadata/TabletServerMetadataCacheTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/metadata/PartitionTombstoneMetadataPropagationTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/InternalIndexWriteGuardTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaManagerTest.java
git commit -m "[index-v2] gate partition writer retirement"
```

## Task 7: Gate the new internal format and isolate it from public KV

**Files:**
- Modify: `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/metadata/TableInfo.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexTableDescriptorFactory.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/TabletServerRegistration.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/TabletServerRegistrationJsonSerde.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/CoordinatorAddress.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/CoordinatorAddressJsonSerde.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/data/ZkData.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/zk/ZooKeeperClient.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletServer.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorServer.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/metadata/ServerInfo.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorContext.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessor.java`
- Create: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/event/ActivateIndexPushFeatureEvent.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/event/watcher/TabletServerChangeWatcher.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java`
- Test: `fluss-common/src/test/java/org/apache/fluss/metadata/TableInfoIndexTableTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/zk/data/TabletServerRegistrationJsonSerdeTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/zk/data/CoordinatorAddressJsonSerdeTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/zk/ZooKeeperClientTest.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/coordinator/CoordinatorServiceIndexCapabilityTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessorTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/coordinator/AutoPartitionManagerTest.java`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java`

- [ ] **Step 1: Write failing isolation tests**

Assert that an internal monotonic batch is rejected by an ordinary table and by an index table without the new push-format property. Assert that the ordinary contiguous writer still rejects a sequence gap and still emits legacy KV/WAL magic.

```java
assertThatThrownBy(() -> ordinaryTablet.put(internalBatch))
        .isInstanceOf(InvalidRecordException.class);
assertThatThrownBy(() -> oldIndexTablet.put(internalBatch))
        .isInstanceOf(InvalidRecordException.class);
assertThatThrownBy(() -> appendOrdinarySequence(1, 3))
        .isInstanceOf(OutOfOrderSequenceException.class);
```

- [ ] **Step 2: Add the internal table property**

Define the system-owned property `table.index-meta.push-format-version` and expose:

```java
public int getIndexPushFormatVersion() {
    return properties.getInt(INDEX_PUSH_FORMAT_VERSION, 0);
}
```

`IndexTableDescriptorFactory` sets it to `1`; user-created tables cannot opt themselves into internal semantics through public DDL validation.

- [ ] **Step 3: Write failing registration, activation, and join-race tests**

```java
assertThat(readV3Registration(jsonV3).features()).isEmpty();
assertThatThrownBy(() -> createIndexedTable(withLegacyLiveServer()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("index-offset-fencing-v1");
assertThatThrownBy(() -> createIndexedTable(withLegacyLiveCoordinator()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("index-offset-fencing-v1");
assertThat(createOrdinaryTable(withLegacyLiveServer())).succeeds();
```

Race a legacy TabletServer joining before, during, and after marker activation. Before activation it blocks index creation; during activation it must be removed before assignment; after activation startup/watcher admission must reject it, so manual partition, auto-partition, and rebalance cannot select it. Assert insufficient compatible replicas fail instead of using the legacy server. Assert a failed table creation does not remove an already-created activation marker.

- [ ] **Step 4: Version registrations and implement the activation barrier**

Upgrade `TabletServerRegistration` JSON to v4 and `CoordinatorAddress` JSON to v3 with a `features` set. Preserve older deserialization as an empty feature set. TabletServer and Coordinator advertise `index-offset-fencing-v1` only when their runtime contains the corresponding new record readers, assignment serde, and guarded control/apply paths. Carry TabletServer features through `ServerInfo`, startup discovery, and `TabletServerChangeWatcher` without changing endpoint semantics.

Add a persistent `/metadata/features/index-offset-fencing-v1` activation marker. On the first indexed-table request, complete all pure descriptor and permission validation, validate all registrations, create the marker idempotently, then submit `ActivateIndexPushFeatureEvent` and wait for the Coordinator event loop to reconcile live state before generating assignments. Marker activation is monotonic and is not rolled back when table creation later fails.

While the marker is absent, a legacy TabletServer remains usable by ordinary tables and causes indexed-table creation to fail. Once present, Coordinator startup and `TabletServerChangeWatcher` do not admit a TabletServer lacking the feature into live assignment or metadata state; activation reconciliation removes one that raced with validation. This cluster admission barrier closes join races without adding per-table eligibility logic to auto-partitioning and rebalance.

- [ ] **Step 5: Enforce the create-time capability gate**

Retain live server feature sets in `CoordinatorContext`; obtain all live Coordinator and TabletServer registrations through `ZooKeeperClient`. `CoordinatorService.createTable` requires every live registration to advertise `index-offset-fencing-v1`, activates and awaits the event-loop barrier, then generates main/index assignments only from reconciled compatible TabletServers and rechecks capability before persisting metadata. Ordinary tables remain unaffected before activation. Unknown internal KV/WAL magic continues to fail closed during read and recovery.

Document the operational contract in code: once an offset-fenced index table exists, downgrading the Coordinator or any hosting TabletServer below this feature is unsupported.

- [ ] **Step 6: Run Task 7 tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=TableInfoIndexTableTest,TabletServerRegistrationJsonSerdeTest,CoordinatorAddressJsonSerdeTest,ZooKeeperClientTest,CoordinatorServiceIndexCapabilityTest,CoordinatorEventProcessorTest,AutoPartitionManagerTest,KvTabletTest test
```

Expected: old registrations remain readable, indexed-table creation is gated, and public KV behavior is byte- and sequence-compatible.

- [ ] **Step 7: Commit Task 7**

```bash
git add fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java \
  fluss-common/src/main/java/org/apache/fluss/metadata/TableInfo.java \
  fluss-common/src/test/java/org/apache/fluss/metadata/TableInfoIndexTableTest.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexTableDescriptorFactory.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/TabletServerRegistration.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/TabletServerRegistrationJsonSerde.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/CoordinatorAddress.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/CoordinatorAddressJsonSerde.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/data/ZkData.java \
  fluss-server/src/main/java/org/apache/fluss/server/zk/ZooKeeperClient.java \
  fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletServer.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorServer.java \
  fluss-server/src/main/java/org/apache/fluss/server/metadata/ServerInfo.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorContext.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessor.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/event/ActivateIndexPushFeatureEvent.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/event/watcher/TabletServerChangeWatcher.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/data/TabletServerRegistrationJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/data/CoordinatorAddressJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/zk/ZooKeeperClientTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/CoordinatorServiceIndexCapabilityTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessorTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/coordinator/AutoPartitionManagerTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java
git commit -m "[index-v2] gate offset-fenced index format"
```

## Task 8: Prove failover correctness with deterministic state-machine tests

**Files:**
- Create: `fluss-server/src/test/java/org/apache/fluss/server/index/OffsetFencedIndexStateMachineTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushFailoverITCase.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushReplicationITCase.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSenderTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorLifecycleTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java`

- [ ] **Step 1: Build a fixed-seed reference model**

Use a small in-test model keyed by `(indexKey, basePrimaryKey, partitionId)` with per `(writerId, targetBucket)` source frontiers. Generate inserts, same-key updates, key-changing updates, deletes, duplicate windows, changed replay boundaries, stale windows, and leader changes from fixed seeds. Do not add a property-testing dependency if the repository does not already have one.

```java
for (long seed : new long[] {1L, 7L, 31L, 127L}) {
    assertImplementationMatchesModel(seed, 10_000);
}
```

- [ ] **Step 2: Reproduce old-leader late arrival explicitly**

Block old leader batch `E=100` before target apply. Elect a new source leader, replay `E=40`, then apply `E=150`. Release the old `E=100` batch last. Assert final index bytes and query results equal the source-table model, the target frontier is `150`, and the late batch performs no KV mutation.

- [ ] **Step 3: Prove the source pipeline invariants**

Use controlled source WAL, sender, and snapshot hooks to assert: uncommitted source WAL is never read; one index has at most one unacknowledged window per source leader; a later window and source progress wait for every target batch acknowledgement; an empty target projection sends no batch but advances source progress; failover restarts at or before the durable all-index minimum; and source WAL retention never passes that minimum.

- [ ] **Step 4: Cover target HW delay and target failover**

Append target state for source `E=200` but hold target HW below it. Submit stale `E=150` and assert no acknowledgement. Fail over the target leader, recover state from WAL, advance HW, and assert stale success is tied to the recovered dominating target offset rather than the request's arrival order.

- [ ] **Step 5: Cover snapshot, segment deletion, and restart combinations**

Exercise all four recovery sources: WAL-only, snapshot plus WAL tail, segment deletion after a covering fsynced snapshot, and unclean restart during snapshot creation. Corrupt and remove the only required covering snapshot after deleting earlier WAL and assert fail-closed recovery. Assert no accepted source frontier regresses and no stale mutation becomes visible.

- [ ] **Step 6: Cover partition drop races end to end**

Write a real partitioned source row and assert the index value and physical key carry pid `10`. Race drop partition against blocked old-leader and replay requests, recreate the logical partition as pid `20`, then release delayed pid-10 UPSERT and DELETE batches. Assert query filtering, compaction filtering, internal write guard, and writer retirement converge to no visible pid-10 rows, cannot delete pid-20 rows, and leave other partitions intact.

- [ ] **Step 7: Cover multi-bucket skew and mutation distributions**

Use source buckets whose offsets differ by orders of magnitude and route them to shared target index buckets. Assert independent writer IDs prevent cross-source comparisons. Include hot and uniform index values, null index columns, repeated same-primary-key updates, multiple same-key mutations in one batch, key-changing updates whose old/new keys route to different target buckets, physical `UPSERT -> DELETE -> UPSERT`, empty windows, retries, and changed window sizes.

- [ ] **Step 8: Complete the ambiguous-failure matrix**

Inject failures before prewrite, after prewrite, during target WAL append, after target WAL append, after writer-state update, and before/after HW advancement. For every uncertain append, assert local fail-stop, recovery from target WAL, source retry, and exact convergence to the reference model; for failures proven to precede WAL advancement, assert bounded rollback and ordinary retry.

- [ ] **Step 9: Remove fixed sleeps and weak duplicate tests**

Replace every `Thread.sleep` in the affected index tests with `waitUntil`, latches, mock callbacks, or exact HW/LEO predicates. Remove tests that only repeat a stronger state-machine assertion without exercising a distinct invariant.

- [ ] **Step 10: Run the focused suite three times**

```bash
for run in 1 2 3; do
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
    -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
    -Dtest=OffsetFencedIndexStateMachineTest,IndexPushFailoverITCase,IndexPushReplicationITCase,IndexSenderTest,IndexReplicatorLifecycleTest,ReplicaTest test || exit 1
done
```

Expected: all fixed seeds and controlled interleavings pass in all three runs without time-based synchronization.

- [ ] **Step 11: Commit Task 8**

```bash
git add fluss-server/src/test/java/org/apache/fluss/server/index \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java
git commit -m "[index-v2] prove offset-fenced failover"
```

## Task 9: Measure cost, update the FIP, and run the full quality gate

**Files:**
- Create: `fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexPushBenchmark.java`
- Create: `fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexWriterStateBenchmark.java`
- Create: `fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexWriterStateCapacityBenchmark.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/metrics/MetricNames.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/metrics/group/TabletServerMetricGroup.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/WriterStateManager.java`
- Modify outside repository: `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md`

- [ ] **Step 1: Add production diagnostics**

Expose counters for accepted internal batches, stale batches, tombstone no-ops, metadata-not-ready rejections, ambiguous append fail-stops, writer frontier count, snapshot bytes, and snapshot duration. Keep labels bounded to table/bucket dimensions already used by TabletServer metrics; never label by writer ID or partition ID.

- [ ] **Step 2: Add focused microbenchmarks**

`IndexWriterStateBenchmark` measures ordinary contiguous validation, accepted internal monotonic validation, stale fast path, physical UPSERT, mixed UPSERT/DELETE, and writer-snapshot serialization at `1`, `1_000`, and `100_000` internal frontiers. Use prebuilt records so writer-state methods isolate validation overhead; mutation methods report RocksDB Get count and allocation with the JMH GC profiler.

```java
@Benchmark
public LogAppendInfo staleInternalBatch(BenchmarkState state) {
    return state.logTablet.append(state.staleBatch);
}
```

- [ ] **Step 3: Add a stable public-API baseline and capacity runner**

`IndexPushBenchmark` drives table creation, main-table writes, index push, lookup, and deletes only through APIs shared by the frozen rejected design and the final implementation. `IndexWriterStateCapacityBenchmark` records retained heap after full GC, frontier count, snapshot bytes, snapshot wall time, recovery time, recovery peak heap, and fresh/stale P50/P95/P99 at increasing topology sizes. Warm up before recording and write machine/JVM/config metadata beside every result.

- [ ] **Step 4: Run rejected and offset-fenced implementations on the same machine**

Copy only the stable public-API benchmark into the frozen Task 0 tree, then run baseline and final code sequentially with the same JDK, Maven cache, JVM flags, data sizes, and iteration counts. Store outputs outside either source tree:

```bash
RESULTS=/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-benchmark-results
BASELINE=/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/.index-v2-rejected-baseline-20260710
mkdir -p "$RESULTS" "$BASELINE/fluss-jmh/src/test/java/org/apache/fluss/jmh"
cp fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexPushBenchmark.java \
  "$BASELINE/fluss-jmh/src/test/java/org/apache/fluss/jmh/"
mvn -o -Dmaven.repo.local=.cache -pl fluss-jmh -am \
  -Dspotless.check.skip=true -DskipTests test-compile
mvn -o -Dmaven.repo.local=.cache -pl fluss-jmh \
  -DskipTests -Dexec.mainClass=org.openjdk.jmh.Main -Dexec.classpathScope=test \
  -Dexec.args="IndexPushBenchmark -prof gc -rf json -rff $RESULTS/offset-fenced-index-push.json" exec:java
mvn -o -Dmaven.repo.local=.cache -f "$BASELINE/pom.xml" -pl fluss-jmh -am \
  -Dspotless.check.skip=true -DskipTests test-compile
mvn -o -Dmaven.repo.local=.cache -f "$BASELINE/pom.xml" -pl fluss-jmh \
  -DskipTests -Dexec.mainClass=org.openjdk.jmh.Main -Dexec.classpathScope=test \
  -Dexec.args="IndexPushBenchmark -prof gc -rf json -rff $RESULTS/rejected-index-push.json" exec:java
```

- [ ] **Step 5: Establish and record performance acceptance criteria**

Record raw JMH JSON and capacity CSV. The merge gate is:

- Ordinary KV throughput regression versus the frozen baseline is less than 2%.
- Accepted internal validation adds no per-row allocation and remains O(1) per target batch.
- Pure physical index UPSERT performs zero RocksDB Gets; stale batches perform zero row decode, prewrite, RocksDB Get, and WAL I/O.
- Fresh and stale throughput plus mixed UPSERT/DELETE P99 are recorded against the rejected implementation.
- Snapshot bytes/time, recovery time/peak heap, and retained heap scale linearly through at least 100,000 frontiers.
- The reviewed FIP records a supported maximum frontier topology and explicit memory, snapshot-pause, recovery-time, and P99 limits derived from the curves.

If a numeric threshold fails, or capacity limits have not been reviewed and recorded, stop before declaring production readiness. Do not weaken a threshold without measured evidence and an explicit review decision.

- [ ] **Step 6: Revise FIP V2 to the code contract**

Update `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md` to describe physical DELETE/UPSERT, stable writer IDs, exclusive long source end offsets, target WAL/HW-bound stale success, durable monotonic frontiers, partition-aware keys, tombstone baseline/retirement, the monotonic cluster activation marker, capability admission, and unsupported Coordinator downgrade. Remove `__source_offset`, `__index_deleted`, logical visibility, and deterministic replay-window claims.

The FIP is outside the Git repository and is therefore verified separately and not included in the repository commit.

- [ ] **Step 7: Scan for dead design artifacts and fixed sleeps**

```bash
rg -n "__source_offset|__index_deleted|IndexEntryVisibilityFilter" \
  fluss-common fluss-server fluss-flink
rg -n "MergeEngineType\.VERSIONED|DeleteBehavior\.IGNORE" \
  fluss-common/src/main/java/org/apache/fluss/utils \
  fluss-server/src/main/java/org/apache/fluss/server/index
rg -n "Thread\.sleep" fluss-server/src/test/java/org/apache/fluss/server/index
```

Expected: no obsolete index-push fields/filter/secondary-index merge mode and no fixed sleeps in V2 index tests. Generic versioned-row support outside secondary indexes remains untouched.

- [ ] **Step 8: Run focused module tests**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-rpc,fluss-server -am \
  -Dspotless.check.skip=true test
```

Expected: all common format, metadata, server recovery, index push, and failover tests pass.

- [ ] **Step 9: Run the workspace compile gate**

```bash
mvn clean compile -o -Dmaven.repo.local=.cache \
  -pl '!fluss-lake/fluss-lake-lance,!fluss-dist' \
  -Dspotless.check.skip=true
```

Expected: all included modules compile. If offline resolution alone fails, rerun once without `-o` to populate `.cache`, then rerun the offline command.

- [ ] **Step 10: Run the workspace test gate**

```bash
mvn test -o -Dmaven.repo.local=.cache \
  -pl '!fluss-lake/fluss-lake-lance,!fluss-dist' \
  -Dspotless.check.skip=true
```

Expected: all included tests pass. Preserve complete failure logs for any retry; do not classify an unexplained rerun pass as success.

- [ ] **Step 11: Run and retain internal benchmark results**

```bash
RESULTS=/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-benchmark-results
mkdir -p "$RESULTS"
mvn -o -Dmaven.repo.local=.cache -pl fluss-jmh -am \
  -Dspotless.check.skip=true -DskipTests test-compile
mvn -o -Dmaven.repo.local=.cache -pl fluss-jmh \
  -Dspotless.check.skip=true -DskipTests \
  -Dexec.mainClass=org.openjdk.jmh.Main -Dexec.classpathScope=test \
  -Dexec.args="IndexWriterStateBenchmark -prof gc -rf json -rff $RESULTS/index-writer-state-benchmark.json" \
  exec:java
mvn -o -Dmaven.repo.local=.cache -pl fluss-jmh \
  -Dspotless.check.skip=true -DskipTests \
  -Dexec.mainClass=org.apache.fluss.jmh.IndexWriterStateCapacityBenchmark \
  -Dexec.classpathScope=test -Dexec.args="$RESULTS/index-writer-state-capacity.csv" exec:java
```

Expected: all acceptance criteria in Step 5 hold, and raw results are retained outside the repository.

- [ ] **Step 12: Audit and commit all intended repository changes**

Review every path that was already dirty at Task 0, including snapshot fixes and `docs/index-push-mechanism.html`. Confirm each intended change has focused test evidence, remove only dead rejected-design artifacts, and leave no unreviewed file silently staged.

```bash
git status --short
git diff --check
git add fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexPushBenchmark.java \
  fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexWriterStateBenchmark.java \
  fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexWriterStateCapacityBenchmark.java \
  fluss-common/src/main/java/org/apache/fluss/metrics/MetricNames.java \
  fluss-server/src/main/java/org/apache/fluss/server/metrics/group/TabletServerMetricGroup.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/WriterStateManager.java \
  docs/index-push-mechanism.html \
  fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/CompletedSnapshot.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/KvTabletSnapshotTarget.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/SnapshotLocation.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot/CompletedSnapshotJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot/KvSnapshotDataUploaderTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot/KvTabletSnapshotTargetTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot/SnapshotLocationTest.java
git commit -m "[index-v2] benchmark offset-fenced index push"
```

- [ ] **Step 13: Verify repository accounting**

```bash
git status --short
git diff --check HEAD^
```

Expected: no repository changes remain unstaged or uncommitted, and the complete final commit diff has no whitespace errors. The FIP, frozen baseline, and benchmark results remain intentionally outside the repository.

## Completion Gate

- [ ] Stable source `TableBucket` writer IDs survive reassignment and recovery.
- [ ] Internal source offsets are exclusive `long` end offsets and never share ordinary contiguous writer semantics.
- [ ] Target apply is physical, monotonic, stale-safe, HW-bound, and fail-stop after ambiguous WAL advancement.
- [ ] Partition ID is part of the physical key, tombstone baseline is explicit, and retirement cannot race writer recreation.
- [ ] Ordinary public KV bytes, sequence validation, and table behavior remain unchanged.
- [ ] Mixed-version clusters reject indexed-table activation before internal records can be emitted.
- [ ] Deterministic tests prove old-leader late arrival, target failover, recovery, partition drop, multi-bucket skew, and changed replay boundaries.
- [ ] No obsolete hidden columns, logical visibility code, fixed sleeps, or low-value duplicate tests remain.
- [ ] Full compile/test gates pass and benchmark evidence satisfies the recorded performance thresholds.
- [ ] FIP V2 describes exactly the implemented contract and its downgrade boundary.
