# Index Pushed Offset Initialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure every main table with secondary indexes starts index replication at source WAL offset `0`, always records a non-negative `indexPushedOffset` in its KV Snapshots, and rejects Snapshot metadata that cannot prove a safe recovery offset.

**Architecture:** Keep the current `Replica -> ReplicaIndexController -> IndexReplicator` lifecycle and existing Snapshot format. `Replica` derives whether index progress applies from its immutable schema, `IndexReplicator` accepts only an explicit non-negative next-read offset, and Snapshot recovery validates the existing optional field before downloading state or advancing WAL retention.

**Tech Stack:** Java 8-compatible Fluss server code, JUnit 5, AssertJ, Mockito, Maven, RocksDB KV Snapshots, local and raw remote WAL recovery.

## Global Constraints

- A main table that declares secondary indexes has `allIndexPushedOffset == 0` before any source WAL record is copied.
- A main table with at least one SYNC index also has `syncIndexPushedOffset == 0` initially.
- A table without secondary indexes continues to omit `indexPushedOffset` from KV Snapshot metadata.
- Do not add an RPC, configuration, metadata node, KV Snapshot format version, leader-readiness state, or cross-table coordination path.
- Do not derive missing index progress from `LogTablet.logStartOffset()` or any other storage-retention boundary.
- Validate missing or negative Snapshot progress before Snapshot download, `LogTablet.updateMinRetainOffset()`, KV leader publication, and IndexReplicator startup.
- Preserve existing exact-offset recovery from valid Snapshots and existing raw remote WAL continuity checks.
- Do not modify the separate tombstone read-path issue in this plan.
- Do not add fixed sleeps. Use the existing manually triggered executors, completion waiters, and exact state assertions.
- Run Maven with `JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home`, offline cache `.cache`, and `-Dspotless.check.skip=true` only because the local JDK/Spotless combination already requires it.

---

## File Map

- `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java`
  - Owns the non-negative `initialOffset` contract and removes the `logStartOffset()` fallback.
- `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java`
  - Documents and forwards the now-required non-negative initial offset.
- `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
  - Derives whether index progress applies, initializes progress, includes it in Snapshot state, and validates restored Snapshot metadata.
- `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java`
  - Proves negative initial offsets cannot enter the replicator.
- `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushFailoverITCase.java`
  - Proves `0` is the exact pre-write baseline and remains unchanged while a first SYNC window retries.
- `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTestBase.java`
  - Adds only the test-construction overload needed to combine custom `TableInfo` and `SnapshotContext`.
- `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java`
  - Proves metadata delay, Snapshot-before-replicator retention, invalid Snapshot rejection, cleanup, and no-index compatibility.
- `docs/superpowers/specs/2026-07-15-index-pushed-offset-initialization-design.md`
  - Records final implementation and verification status after all tests pass.
- `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md`
  - States the same initial-offset and Snapshot recovery contract. This file is outside the Git repository and is verified separately.

---

### Task 1: Make Negative IndexReplicator Offsets Unrepresentable

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java:200-224,327-365`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java:145-150,405-420`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java`

**Interfaces:**
- Consumes: `IndexReplicator`'s existing `long initialOffset`, where the value is the exclusive next source WAL offset to read.
- Produces: one constructor invariant, `initialOffset >= 0`; all public and test constructors delegate to the checked constructor.

- [ ] **Step 1: Add the failing constructor-contract test**

Add this focused test to `IndexReplicatorAppendTest`:

```java
@Test
void rejectsNegativeInitialOffset() {
    assertThatThrownBy(
                    () ->
                            new IndexReplicator(
                                    null,
                                    Collections.emptyList(),
                                    new IndexAccumulator(),
                                    null,
                                    -1L,
                                    1024,
                                    (sync, all) -> {}))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("initialOffset must be non-negative, but was -1");
}
```

- [ ] **Step 2: Run the test and prove the current implementation accepts the invalid value**

Run:

```bash
cd /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-index-v2
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexReplicatorAppendTest#rejectsNegativeInitialOffset test
```

Expected: FAIL because construction returns normally instead of throwing `IllegalArgumentException`.

- [ ] **Step 3: Check the offset once in the central constructor**

At the beginning of the package-private constructor to which every overload delegates, add:

```java
if (initialOffset < 0) {
    throw new IllegalArgumentException(
            "initialOffset must be non-negative, but was " + initialOffset);
}
```

Keep the check before any `IndexProgressState`, source reader ownership, or callback state is created.

- [ ] **Step 4: Delete the storage-boundary substitution**

Delete this block from `pollLocked()`:

```java
if (state.pushedOffset < 0) {
    state.pushedOffset = sourceReader.logStartOffset();
}
```

The following line remains the only source of the next read position:

```java
long readOffset = nextReadOffset(state);
```

- [ ] **Step 5: Align controller documentation with the constructor contract**

Replace the `onBecomeLeader` parameter text with:

```java
 * @param initialOffset the non-negative exclusive next source WAL offset restored by the owning
 *     replica
```

Replace the construction comment in `maybeStartIndexReplicator` with:

```java
// Use the owning replica's explicit non-negative next-read offset.
```

- [ ] **Step 6: Run the focused replicator suite**

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexReplicatorAppendTest,IndexReplicatorLifecycleTest,ReplicaIndexControllerTest test
```

Expected: all selected tests pass.

Run the structural audit:

```bash
rg -n "pushedOffset < 0|pushedOffset = sourceReader\.logStartOffset|snapshot restore or -1|initialOffset.*-1L" \
  fluss-server/src/main/java/org/apache/fluss/server/index
```

Expected: no output.

- [ ] **Step 7: Commit the checked component contract**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java
git commit -m "Reject negative index replication offsets"
```

---

### Task 2: Initialize and Persist Index Progress From the Table Schema

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java:243-258,324-336,823-834`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTestBase.java:499-540`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java:555-621,1457-1602`

**Interfaces:**
- Consumes: immutable `TableInfo.getSchema().getIndexes()` and existing `TabletState` nullable `indexPushedOffset`.
- Produces: `Replica.hasSecondaryIndexes`, schema-derived initial values, and Snapshot state that distinguishes "no indexes" from "indexes starting at offset 0".

- [ ] **Step 1: Add failing assertions to the metadata-deferred path**

In `testIndexReplicatorInitDefersWhenIndexTableNotYetInCache()`, after asserting `DEFERRED`, add:

```java
assertThat(f.replica.getAllIndexPushedOffset())
        .as("an indexed main table must retain source WAL from offset zero while deferred")
        .isZero();
assertThat(f.replica.getSyncIndexPushedOffset())
        .as("the first SYNC write must wait beyond the offset-zero baseline")
        .isZero();
```

- [ ] **Step 2: Extend the test factory without changing production APIs**

Add this overload to `ReplicaTestBase`:

```java
protected Replica makeKvReplica(
        PhysicalTablePath physicalTablePath,
        TableBucket tableBucket,
        SnapshotContext snapshotContext,
        TableInfo tableInfo)
        throws Exception {
    return makeReplica(physicalTablePath, tableBucket, true, snapshotContext, tableInfo);
}
```

In `ReplicaTest`, replace the current
`private IndexedFixture setupIndexedMainTableReplica()` signature with
`private IndexedFixture setupIndexedMainTableReplica(TestSnapshotContext snapshotContext)`, keeping
its current schema, `TableInfo`, and ZK registration statements in that method. Add this wrapper
immediately before it:

```java
private IndexedFixture setupIndexedMainTableReplica() throws Exception {
    return setupIndexedMainTableReplica(null);
}
```

At the existing replica-construction statement, use:

```java
Replica replica =
        snapshotContext == null
                ? makeKvReplica(PhysicalTablePath.of(mainPath), mainBucket, mainTableInfo)
                : makeKvReplica(
                        PhysicalTablePath.of(mainPath),
                        mainBucket,
                        snapshotContext,
                        mainTableInfo);
```

The existing return statement remains the end of the custom-context overload.

- [ ] **Step 3: Add a deterministic follower helper for the indexed fixture**

Add beside `makeIndexedMainReplicaAsLeader(...)`:

```java
private void makeIndexedMainReplicaAsFollower(IndexedFixture f, int leaderEpoch) {
    int remoteLeader = TABLET_SERVER_ID + 1;
    List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, remoteLeader);
    f.replica.makeFollower(
            new NotifyLeaderAndIsrData(
                    PhysicalTablePath.of(f.mainPath),
                    new TableBucket(f.mainTableId, 0),
                    replicas,
                    new LeaderAndIsr(
                            remoteLeader,
                            leaderEpoch,
                            replicas,
                            Collections.emptyList(),
                            INITIAL_COORDINATOR_EPOCH,
                            leaderEpoch)));
}
```

The non-local leader id is important: after the call, `Replica.isLeader()` must be false and its KV tablet must have been dropped.

- [ ] **Step 4: Add the failing Snapshot-before-IndexReplicator regression test**

Add this test to `ReplicaTest`:

```java
@Test
void testSnapshotBeforeIndexReplicatorRetainsSourceWalFromZero(@TempDir Path snapshotDir)
        throws Exception {
    TestSnapshotContext snapshotContext = new TestSnapshotContext(snapshotDir.toString());
    IndexedFixture f = setupIndexedMainTableReplica(snapshotContext);
    TableBucket sourceBucket = new TableBucket(f.mainTableId, 0);

    makeIndexedMainReplicaAsLeader(f, 0);
    assertThat(f.replica.isIndexReplicatorInitDeferred()).isTrue();
    putRecordsToLeader(
            f.replica,
            org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch(
                    new Object[] {1, "before-snapshot"}));
    long expectedLogOffset = f.replica.getLocalLogEndOffset();
    assertThat(expectedLogOffset).isPositive();

    makeIndexedMainReplicaAsFollower(f, 1);
    assertThat(f.replica.isLeader()).isFalse();
    assertThat(f.replica.getKvTablet()).isNull();

    AtomicReference<CompletedSnapshot> snapshotTakenBeforeReplicator = new AtomicReference<>();
    f.replica.setKvSnapshotInitializationFaultInjector(
            manager -> {
                assertThat(f.replica.getIndexReplicator()).isNull();
                manager.triggerSnapshot();
                snapshotTakenBeforeReplicator.set(
                        snapshotContext.testKvSnapshotStore.waitUntilSnapshotComplete(
                                sourceBucket, 0));
            });

    makeIndexedMainReplicaAsLeader(f, 2);

    CompletedSnapshot snapshot = snapshotTakenBeforeReplicator.get();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.getLogOffset()).isEqualTo(expectedLogOffset);
    assertThat(snapshot.getIndexPushedOffset()).isEqualTo(0L);
    assertThat(snapshot.getMinRetainLogOffset()).isZero();
    assertThat(f.replica.getLogTablet().getMinRetainOffset()).isZero();
    assertThat(f.replica.isIndexReplicatorInitDeferred()).isTrue();
    assertThat(f.replica.getIndexReplicator()).isNull();
}
```

This test creates committed source WAL on the first leader run, then takes the Snapshot from the existing hook after the second Snapshot manager starts but before `onLeaderKvReady()` can create or defer the replacement IndexReplicator.

- [ ] **Step 5: Run the tests and prove both current defects**

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='ReplicaTest#testIndexReplicatorInitDefersWhenIndexTableNotYetInCache+testSnapshotBeforeIndexReplicatorRetainsSourceWalFromZero' test
```

Expected: FAIL because the indexed replica exposes `-1`, and the Snapshot omits `indexPushedOffset` or retains WAL only from its positive KV log offset.

- [ ] **Step 6: Implement schema-derived progress initialization**

In `Replica`, add:

```java
/** Whether this main-table schema declares at least one secondary index. */
private final boolean hasSecondaryIndexes;
```

Remove the `= -1L` field initializers so the constructor owns the complete decision:

```java
private volatile long syncIndexPushedOffset;
private volatile long allIndexPushedOffset;
```

Replace the current `hasSyncIndexes` assignment with:

```java
List<Schema.Index> secondaryIndexes = tableInfo.getSchema().getIndexes();
this.hasSecondaryIndexes = !secondaryIndexes.isEmpty();
this.hasSyncIndexes =
        secondaryIndexes.stream()
                .anyMatch(index -> index.getVisibility() == IndexVisibility.SYNC);
this.syncIndexPushedOffset = hasSyncIndexes ? 0L : -1L;
this.allIndexPushedOffset = hasSecondaryIndexes ? 0L : -1L;
```

Index Tables and ordinary KV tables have no declared secondary indexes, so their existing not-applicable representation remains unchanged.

- [ ] **Step 7: Make Snapshot inclusion depend on the schema**

Replace `augmentTabletState(...)` with:

```java
private TabletState augmentTabletState(TabletState base) {
    Long indexPushedOffset = null;
    if (hasSecondaryIndexes) {
        long currentOffset = allIndexPushedOffset;
        if (currentOffset < 0) {
            throw new IllegalStateException(
                    "Indexed main table "
                            + tableBucket
                            + " has invalid allIndexPushedOffset "
                            + currentOffset);
        }
        indexPushedOffset = currentOffset;
    }
    return new TabletState(
            base.getFlushedLogOffset(),
            base.getRowCount(),
            indexPushedOffset,
            base.getAutoIncIDRanges());
}
```

Do not change `TabletState.getMinRetainLogOffset()`: its nullable behavior is still correct for tables without secondary indexes.

- [ ] **Step 8: Run Replica tests and the no-index Snapshot suite**

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=ReplicaTest,TabletStateTest,KvTabletSnapshotTargetTest test
```

Expected: all selected tests pass except existing indexed tests that still assert the old `-1` baseline; those are intentionally corrected in Task 4.

- [ ] **Step 9: Commit initialization and Snapshot persistence**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTestBase.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java
git commit -m "Initialize index replication progress at zero"
```

---

### Task 3: Reject Main-Table KV Snapshots Without Valid Index Progress

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java:1035-1072`
- Test: `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java`

**Interfaces:**
- Consumes: `CompletedSnapshot.getIndexPushedOffset()` and `hasSecondaryIndexes` from Task 2.
- Produces: `validateSnapshotIndexPushedOffset(CompletedSnapshot)`, called before Snapshot download and any WAL-retention update.

- [ ] **Step 1: Add invalid-Snapshot test data**

Add this source beside the existing `putKvProtocolMatrix()` source in `ReplicaTest`:

```java
private static Stream<Arguments> invalidSnapshotIndexPushedOffsets() {
    return Stream.of(Arguments.of((Long) null), Arguments.of(-1L));
}
```

Import `KvSnapshotHandle`, plus Mockito's `mock` and `verifyNoInteractions` static methods.

- [ ] **Step 2: Add the failing full leader-recovery test**

Add:

```java
@ParameterizedTest(name = "indexPushedOffset={0}")
@MethodSource("invalidSnapshotIndexPushedOffsets")
void testIndexedMainRejectsSnapshotWithoutValidIndexPushedOffset(
        Long indexPushedOffset, @TempDir Path snapshotDir) throws Exception {
    TestSnapshotContext snapshotContext = new TestSnapshotContext(snapshotDir.toString());
    IndexedFixture f = setupIndexedMainTableReplica(snapshotContext);
    TableBucket sourceBucket = new TableBucket(f.mainTableId, 0);
    KvSnapshotHandle snapshotHandle = mock(KvSnapshotHandle.class);
    CompletedSnapshot invalidSnapshot =
            new CompletedSnapshot(
                    sourceBucket,
                    0L,
                    new FsPath(snapshotDir.toUri()),
                    snapshotHandle,
                    5L,
                    1L,
                    indexPushedOffset,
                    null);
    snapshotContext.testKvSnapshotStore.commitKvSnapshot(
            invalidSnapshot, INITIAL_COORDINATOR_EPOCH, INITIAL_LEADER_EPOCH);

    assertThatThrownBy(() -> makeIndexedMainReplicaAsLeader(f))
            .isInstanceOf(KvStorageException.class)
            .hasRootCauseMessage(
                    "KV Snapshot 0 for main table bucket "
                            + sourceBucket
                            + " with secondary indexes must contain a non-negative "
                            + "indexPushedOffset, but was "
                            + indexPushedOffset);

    verifyNoInteractions(snapshotHandle);
    assertThat(f.replica.isLeader()).isFalse();
    assertThat(f.replica.getKvTablet()).isNull();
    assertThat(kvManager.getKv(sourceBucket)).isEmpty();
    assertThat(f.replica.getKvSnapshotManager()).isNull();
    assertThat(f.replica.hasReadyKvSnapshotManager()).isFalse();
    assertThat(f.replica.getIndexReplicator()).isNull();
    assertThat(f.replica.getLogTablet().getMinRetainOffset()).isZero();
}
```

The mock handle proves validation occurs before the download path touches Snapshot data. The resource assertions prove all five retry attempts clean up without publishing a partial leader.

- [ ] **Step 3: Run the test and verify the current failure is not the required validation**

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=ReplicaTest#testIndexedMainRejectsSnapshotWithoutValidIndexPushedOffset test
```

Expected: FAIL because the current code enters Snapshot download instead of reporting the explicit missing/negative `indexPushedOffset` contract.

- [ ] **Step 4: Add and call the schema-aware validation**

Add to `Replica`:

```java
private void validateSnapshotIndexPushedOffset(CompletedSnapshot snapshot) {
    if (!hasSecondaryIndexes) {
        return;
    }
    Long indexPushedOffset = snapshot.getIndexPushedOffset();
    if (indexPushedOffset == null || indexPushedOffset < 0) {
        throw new KvStorageException(
                "KV Snapshot "
                        + snapshot.getSnapshotID()
                        + " for main table bucket "
                        + tableBucket
                        + " with secondary indexes must contain a non-negative "
                        + "indexPushedOffset, but was "
                        + indexPushedOffset);
    }
}
```

In the `optCompletedSnapshot.isPresent()` branch, call it immediately after obtaining `completedSnapshot` and before creating a tablet directory or calling `downloadKvSnapshots(...)`:

```java
CompletedSnapshot completedSnapshot = optCompletedSnapshot.get();
validateSnapshotIndexPushedOffset(completedSnapshot);
```

Leave the existing non-null seed after KV load as:

```java
Long snapshotIndexOffset = completedSnapshot.getIndexPushedOffset();
if (snapshotIndexOffset != null) {
    seedIndexPushedOffsetOnLoad(snapshotIndexOffset);
}
```

This preserves existing behavior for no-index tables that read old Snapshot metadata.

- [ ] **Step 5: Strengthen the existing no-index Snapshot test**

In `testKvReplicaSnapshot`, immediately after `completedSnapshot0` completes, add:

```java
assertThat(completedSnapshot0.getIndexPushedOffset()).isNull();
assertThat(completedSnapshot0.getMinRetainLogOffset())
        .isEqualTo(completedSnapshot0.getLogOffset());
```

The existing demote, recreate, and restore sequence in the same test remains the proof that a no-index Snapshot without this field is valid.

- [ ] **Step 6: Run recovery and cleanup tests**

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=ReplicaTest,ReplicaLeaderTransitionTest,CompletedSnapshotJsonSerdeTest,TabletStateTest test
```

Expected: all selected tests pass, including both invalid values and the no-index restore path.

- [ ] **Step 7: Commit fail-closed Snapshot recovery**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java
git commit -m "Reject invalid index snapshot progress"
```

---

### Task 4: Align Failover Assertions, FIP, and Final Verification

**Files:**
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushFailoverITCase.java:300-315,418-428`
- Modify: `docs/superpowers/specs/2026-07-15-index-pushed-offset-initialization-design.md:3-6`
- Modify: `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md:317-343,463-480,849-890`

**Interfaces:**
- Consumes: the zero-offset and Snapshot validation contracts implemented in Tasks 1-3.
- Produces: production-shaped exact assertions and FIP text matching the code.

- [ ] **Step 1: Replace the obsolete failover sentinel assertions**

In `IndexPushFailoverITCase`, replace the first pre-write assertion with:

```java
// Offset zero is the exclusive next-read position before the first source record.
assertThat(mainReplica.getSyncIndexPushedOffset())
        .as("the first SYNC write must start from the offset-zero baseline")
        .isZero();
assertThat(mainReplica.getAllIndexPushedOffset()).isZero();
```

While the target bucket is unavailable, replace the retry assertion with:

```java
assertThat(mainReplica.getSyncIndexPushedOffset())
        .as("sync progress must remain at the pre-write baseline during retry")
        .isZero();
assertThat(mainReplica.getAllIndexPushedOffset()).isZero();
```

Keep the later exact transition to `1L`. It proves the first source record advances the exclusive next-read offset only after the target batch receives its committed response.

- [ ] **Step 2: Update the FIP with the exact code contract**

After the existing statement that offsets denote the next source WAL offset to read, add:

```markdown
声明二级索引的主表从 `allIndexPushedOffset = 0` 开始；若至少存在一个 SYNC
索引，`syncIndexPushedOffset` 也从 `0` 开始。`0` 表示尚未复制任何 source WAL
记录且下一条待读取记录位于 offset 0，不使用负数哨兵。
```

Add this invariant after the existing Snapshot/WAL-retention invariant:

```markdown
- **INV-8**：声明二级索引的主表所提交的每个 KV Snapshot 都必须记录非负
  `allIndexPushedOffset`；字段缺失或为负时 leader KV 恢复失败。
```

In both source leader recovery sections, state these two distinct cases exactly:

```markdown
- 不存在主表 KV Snapshot 时，索引复制从 source WAL offset `0` 开始。
- schema 声明二级索引但主表 KV Snapshot 缺少非负 `allIndexPushedOffset` 时，
  Snapshot 状态不完整，leader 恢复失败；不得用当前 `logStartOffset` 代替。
```

Do not introduce a new name for the Snapshot or for either recovery case.

- [ ] **Step 3: Run the production-shaped index recovery tests**

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexPushFailoverITCase,IndexSourceRemoteRecoveryITCase,IndexPushOrderingITCase test
```

Expected: all selected tests pass. In particular, the first SYNC write begins at `0`, target failover leaves progress at `0` during retry, valid Snapshot recovery restores its exact positive offset, and remote source WAL recovery still catches up without skipping.

- [ ] **Step 4: Run the complete focused regression family**

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='Index*Test,Index*ITCase,ReplicaTest,ReplicaLeaderTransitionTest,TabletStateTest,KvTabletSnapshotTargetTest,CompletedSnapshotJsonSerdeTest' test
```

Expected: all selected tests pass with zero failures and zero errors.

- [ ] **Step 5: Mark the approved design implemented after focused verification passes**

Only after Steps 3 and 4 pass, replace the design status with:

```markdown
The design was accepted in discussion on 2026-07-15 and implemented after the focused and
production-shaped verification listed below passed.
```

- [ ] **Step 6: Audit fixed sleeps, obsolete semantics, and formatting**

Run:

```bash
rg -n "Thread\.sleep|TimeUnit\.[A-Z]+\.sleep" \
  fluss-server/src/test/java/org/apache/fluss/server/index \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java
```

Expected: no output.

Run:

```bash
rg -n "initial sentinel|nothing pushed.*-1|snapshot restore or -1|pushedOffset = sourceReader\.logStartOffset" \
  fluss-server/src/main fluss-server/src/test \
  /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md
```

Expected: no output. The unrelated no-index delayed-write sentinel tests may still use `-1L` because index progress does not apply to those tables.

Run:

```bash
git diff --check
```

Expected: no output.

- [ ] **Step 7: Run workspace acceptance commands**

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn clean compile -o -Dmaven.repo.local=.cache \
  -Dspotless.check.skip=true \
  -pl '!fluss-lake/fluss-lake-lance,!fluss-dist'
```

Expected: `BUILD SUCCESS`. If offline mode reports only missing artifacts, rerun once without `-o` to populate `.cache`, then repeat the offline command.

Run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn test -o -Dmaven.repo.local=.cache \
  -Dspotless.check.skip=true \
  -pl '!fluss-lake/fluss-lake-lance,!fluss-dist'
```

Expected: `BUILD SUCCESS`. Report any unrelated pre-existing failure instead of weakening or deleting tests.

- [ ] **Step 8: Commit tracked assertion and documentation alignment**

```bash
git add fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushFailoverITCase.java \
  docs/superpowers/specs/2026-07-15-index-pushed-offset-initialization-design.md
git commit -m "Align index progress recovery contract"
```

The FIP file is outside this Git repository. Verify its saved contents with:

```bash
rg -n 'allIndexPushedOffset = 0|INV-8|不得用当前 `logStartOffset` 代替' \
  /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md
```

Expected: one matching initialization rule, one invariant, and the recovery rejection rule.

- [ ] **Step 9: Final history and worktree audit**

Run:

```bash
git status --short --branch
git log -5 --oneline
```

Expected: no uncommitted tracked or untracked implementation files. The latest commits correspond to the four task boundaries above and contain no co-author trailer.
