# Index Source Remote Recovery Test Maintainability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `IndexSourceRemoteRecoveryITCase` easier to audit by giving mutable resources one owner, removing hidden fixture dependencies, and expressing the existing recovery proof as named causal phases.

**Architecture:** Keep the existing single integration test and every production-facing assertion. Replace the class-level generic fixture with an explicit `RemoteRecoveryFixture` that owns cleanup, then extract setup and execution phases inside the same test file. No production API or shared test framework changes.

**Tech Stack:** Java 8-compatible Fluss test code, JUnit 5, AssertJ, `FlussClusterExtension`, Maven, x86_64 Microsoft OpenJDK 17, cached RocksDB JNI.

## Global Constraints

- Modify only `IndexSourceRemoteRecoveryITCase.java` plus the approved design status; do not change
  production code.
- Preserve one `@Test` and the current four-TabletServer topology, workload, timeouts, replay gate, and exact physical projection assertions.
- Keep remote diagnostics outside the polling hot predicate.
- Do not introduce a shared failover DSL, common test base, fixed sleep, or line-count target.
- Use `setUpRemoteRecovery()` returning `RemoteRecoveryFixture`; do not retain `createFixture()` or a class-level `fixture` field.
- Run Maven with `JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home` so RocksDB loads the cached x86_64 JNI library.

---

### Task 1: Give Recovery Resources One Explicit Owner

**Files:**
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceRemoteRecoveryITCase.java:121-905`

**Interfaces:**
- Consumes: existing `ReplayGate`, `stopAndTrack(...)`, `startAndUntrack(...)`, `cleanup(...)`, and `reportCleanupFailures(...)` behavior.
- Produces: `RemoteRecoveryFixture implements AutoCloseable`, `RemoteRecoveryFixture.stopServer(int)`, `startServer(int)`, `installReplayGate(Replica, WriterKey, long)`, `replayGate()`, and explicit fixture parameters on all helpers that currently read the class-level field.

- [ ] **Step 1: Establish the focused behavioral baseline**

Run:

```bash
cd /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-index-v2
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexSourceRemoteRecoveryITCase test
```

Expected: 1 test, 0 failures, 0 errors, 0 skips.

- [ ] **Step 2: Replace the generic fixture type and hidden class state**

Delete:

```java
private Fixture fixture;
```

Rename the current `Fixture` data holder to `RemoteRecoveryFixture` and add resource ownership:

```java
private static final class RemoteRecoveryFixture implements AutoCloseable {
    private final long mainTableId;
    private final long indexTableId;
    private final TableBucket sourceTableBucket;
    private final int sourceLeader;
    private final int recoveryFollower;
    private final int offlineFollower;
    private final int targetOnlyServer;
    private final TableBucket gatedTargetBucket;
    private final String firstIndexValue;
    private final String secondIndexValue;
    private final Replica sourceReplica;
    private final Set<Integer> stoppedServers = new LinkedHashSet<>();
    @Nullable private ReplayGate replayGate;

    private void stopServer(int serverId) throws Exception {
        stopAndTrack(serverId, stoppedServers);
    }

    private void startServer(int serverId) throws Exception {
        startAndUntrack(serverId, stoppedServers);
    }

    private ReplayGate installReplayGate(
            Replica targetReplica, WriterKey writerKey, long baselineSequence) {
        assertThat(replayGate).as("a replay gate may be installed only once").isNull();
        replayGate = new ReplayGate(targetReplica, writerKey, baselineSequence);
        return replayGate;
    }

    private ReplayGate replayGate() {
        assertThat(replayGate).as("the replay gate must be installed").isNotNull();
        return replayGate;
    }

    @Override
    public void close() {
        List<Throwable> failures = new ArrayList<>();
        if (replayGate != null) {
            replayGate.release();
        }
        cleanup(failures, () -> closeGate(replayGate));
        for (int stoppedServer : new LinkedHashSet<>(stoppedServers)) {
            cleanup(failures, () -> startAndUntrack(stoppedServer, stoppedServers));
        }
        cleanup(failures, () -> CLUSTER.assertHasTabletServerNumber(TABLET_SERVER_COUNT));
        reportCleanupFailures(null, failures);
    }
}
```

Rename `createFixture()` to `setUpRemoteRecovery()` at the same time, without decomposing its body
until Task 2.

Wrap the existing statements from the first follower stop through the evidence log in:

```java
try (RemoteRecoveryFixture recovery = setUpRemoteRecovery()) {
    // Statements currently at lines 131-288 are indented here without reordering.
}
```

During implementation, the comment above is replaced by those exact existing statements, not
retained in source. Java try-with-resources replaces the current top-level `primaryFailure`,
`cleanupFailures`, nullable local gate, and `finally` block. A `close()` assertion is automatically
suppressed onto a primary test failure.

- [ ] **Step 3: Make every fixture dependency explicit**

Update helper signatures and callers exactly as follows:

```java
private static void putSourceRow(
        RemoteRecoveryFixture recovery, int sourceLeader, int key, String indexedValue)
        throws Exception;

private static void waitForRawRemoteReplayCoverage(
        RemoteRecoveryFixture recovery,
        Replica sourceReplica,
        long baselinePushedOffset,
        long committedSourceEnd);

private static void waitForExactPhysicalRows(
        RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception;

private static void assertExactIndexProjection(
        RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception;

private static void assertIndexKeyAbsent(
        RemoteRecoveryFixture recovery,
        String indexedValue,
        int primaryKey,
        String description)
        throws Exception;

private static TableInfo liveIndexTableInfo(RemoteRecoveryFixture recovery);

private static Map<TableBucket, Map<String, String>> emptyIndexProjection(
        RemoteRecoveryFixture recovery);
```

Inside each method, replace reads of `fixture` with the explicit `recovery` parameter. Keep `waitForSourceCommit(...)`, encoding helpers, diagnostics, and static cluster access unchanged.

- [ ] **Step 4: Verify resource ownership did not change behavior**

Run the focused baseline command from Step 1 again.

Expected: 1 test, 0 failures, 0 errors, 0 skips. On both success and failure paths, all stopped TabletServers are restarted and the replay hook is unregistered.

Run:

```bash
rg -n 'private (Fixture|RemoteRecoveryFixture) fixture|createFixture\(' \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceRemoteRecoveryITCase.java
git diff --check
```

Expected: `rg` prints no matches; `git diff --check` prints no output.

- [ ] **Step 5: Commit the lifecycle refactor**

```bash
git add fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceRemoteRecoveryITCase.java
git commit -m "Own remote recovery test resources"
```

---

### Task 2: Expose Setup Cost and the Five Recovery Phases

**Files:**
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceRemoteRecoveryITCase.java:123-461`
- Modify: `docs/superpowers/specs/2026-07-15-index-source-remote-recovery-test-maintainability-design.md`

**Interfaces:**
- Consumes: Task 1's `RemoteRecoveryFixture` resource owner and explicit helper signatures.
- Produces: decomposed `setUpRemoteRecovery()`, `createIndexedSourceTable()`,
  `resolveRecoveryRoles(...)`, `findTargetBucketLedBy(...)`, five named causal phase methods, and a
  concise top-level test.

- [ ] **Step 1: Rename and decompose active setup**

Start from Task 1's method:

```java
private static RemoteRecoveryFixture setUpRemoteRecovery() throws Exception;
```

Extract these private data holders and helpers:

```java
private static IndexedSourceTable createIndexedSourceTable() throws Exception;

private static RecoveryRoles resolveRecoveryRoles(
        TableBucket sourceTableBucket, int sourceLeader) throws Exception;

private static TableBucket findTargetBucketLedBy(
        long indexTableId, int targetOnlyServer, List<Integer> sourceReplicas)
        throws Exception;

private static final class IndexedSourceTable {
    private final long mainTableId;
    private final long indexTableId;
    private final TableBucket sourceTableBucket;

    private IndexedSourceTable(long mainTableId, long indexTableId) {
        this.mainTableId = mainTableId;
        this.indexTableId = indexTableId;
        this.sourceTableBucket = new TableBucket(mainTableId, 0);
    }
}

private static final class RecoveryRoles {
    private final List<Integer> sourceReplicas;
    private final int sourceLeader;
    private final int recoveryFollower;
    private final int offlineFollower;
    private final int targetOnlyServer;

    private RecoveryRoles(
            List<Integer> sourceReplicas,
            int sourceLeader,
            int recoveryFollower,
            int offlineFollower,
            int targetOnlyServer) {
        this.sourceReplicas = new ArrayList<>(sourceReplicas);
        this.sourceLeader = sourceLeader;
        this.recoveryFollower = recoveryFollower;
        this.offlineFollower = offlineFollower;
        this.targetOnlyServer = targetOnlyServer;
    }
}
```

`createIndexedSourceTable()` owns the current unique table name, paths, schema, table creation, and
Index Table metadata lookup. `resolveRecoveryRoles(...)` owns the current RF3 assignment and
four-distinct-role assertions. Pass `roles.sourceReplicas` into
`findTargetBucketLedBy(...)`; that helper owns all Index Table ISR waits, leader collection, target
selection, and the existing diagnostic failure containing source replicas and all index leaders.
Do not remove any setup assertion.

- [ ] **Step 2: Extract the baseline and replay-range phases**

Extract the current baseline and backlog bodies into:

```java
private static long persistBaselineSnapshot(
        RemoteRecoveryFixture recovery, Map<Integer, String> expectedRows) throws Exception;

private static long createRemoteReplayRange(
        RemoteRecoveryFixture recovery,
        long baselinePushedOffset,
        Map<Integer, String> expectedRows)
        throws Exception;
```

`persistBaselineSnapshot(...)` must stop both selected followers, write and physically verify all baseline rows, wait for exact pushed-offset equality, trigger the snapshot, assert both snapshot offsets equal the baseline, and return `baselinePushedOffset`.

`createRemoteReplayRange(...)` must close and unregister the old replicator, update the first six keys, insert the twelve replay-only keys, wait for the source high watermark, assert pushed progress remains at the baseline, roll exactly at `committedSourceEnd`, wait for both remote endpoints, and return `committedSourceEnd`.

- [ ] **Step 3: Extract failover, recovered projection, and continuation phases**

Add a result holder for values needed by the final evidence log:

```java
private static final class RecoveredSource {
    private final Replica replica;
    private final long followerLocalStart;
    private final long remoteBytesBefore;
    private final long remoteBytesWhileAckHeld;

    private RecoveredSource(
            Replica replica,
            long followerLocalStart,
            long remoteBytesBefore,
            long remoteBytesWhileAckHeld) {
        this.replica = replica;
        this.followerLocalStart = followerLocalStart;
        this.remoteBytesBefore = remoteBytesBefore;
        this.remoteBytesWhileAckHeld = remoteBytesWhileAckHeld;
    }
}
```

Extract the remaining phases with these signatures:

```java
private static RecoveredSource recoverFromRemoteWal(
        RemoteRecoveryFixture recovery,
        long baselinePushedOffset,
        long committedSourceEnd)
        throws Exception;

private static void verifyRecoveredProjection(
        RemoteRecoveryFixture recovery,
        RecoveredSource recovered,
        long committedSourceEnd,
        Map<Integer, String> expectedRows)
        throws Exception;

private static long verifyLocalContinuation(
        RemoteRecoveryFixture recovery,
        RecoveredSource recovered,
        long committedSourceEnd,
        Map<Integer, String> expectedRows)
        throws Exception;
```

`recoverFromRemoteWal(...)` restarts the recovery follower, proves its local start excludes the baseline, installs the replay gate, records remote-read bytes, stops the old leader, waits for the selected follower to lead, waits for gate admission, and proves all three facts while ACK is held: local WAL cannot contain the replay start, pushed offset remains at baseline, and remote-read bytes increase.

`verifyRecoveredProjection(...)` releases the fixture-owned gate, waits for exact `committedSourceEnd`, compares the complete physical projection, and verifies both the stale key and never-written key are absent.

`verifyLocalContinuation(...)` writes the next key through the recovered leader, waits for source commit and exact pushed-offset equality, rechecks the complete physical projection, and returns the continuation end offset.

- [ ] **Step 4: Reduce the top-level test to an auditable protocol**

The complete top-level body becomes:

```java
@Test
void recoverAsyncIndexFromRawRemoteSourceWalAndContinueLocally() throws Throwable {
    try (RemoteRecoveryFixture recovery = setUpRemoteRecovery()) {
        Map<Integer, String> expectedRows = new LinkedHashMap<>();
        long baselinePushedOffset = persistBaselineSnapshot(recovery, expectedRows);
        long committedSourceEnd =
                createRemoteReplayRange(recovery, baselinePushedOffset, expectedRows);
        RecoveredSource recovered =
                recoverFromRemoteWal(recovery, baselinePushedOffset, committedSourceEnd);
        verifyRecoveredProjection(recovery, recovered, committedSourceEnd, expectedRows);
        long continuationEnd =
                verifyLocalContinuation(recovery, recovered, committedSourceEnd, expectedRows);

        LOG.info(
                "Remote index recovery evidence: sourceLeader={}, recoveryFollower={}, "
                        + "offlineFollower={}, targetOnly={}, targetBucket={}; baseline={}, "
                        + "committedEnd={}, followerLocalStart={}, replaySequence={}, "
                        + "remoteBytes={}->{}, continuationEnd={}",
                recovery.sourceLeader,
                recovery.recoveryFollower,
                recovery.offlineFollower,
                recovery.targetOnlyServer,
                recovery.gatedTargetBucket,
                baselinePushedOffset,
                committedSourceEnd,
                recovered.followerLocalStart,
                recovery.replayGate().admittedSequence(),
                recovered.remoteBytesBefore,
                recovered.remoteBytesWhileAckHeld,
                continuationEnd);
    }
}
```

Keep assertion descriptions and timeout diagnostics from the original bodies unchanged. Apply the repository formatter only to the modified Java file.

- [ ] **Step 5: Run focused, repeated, and neighboring verification**

Focused run:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexSourceRemoteRecoveryITCase test
```

Expected: 1/1 passes.

Three-run stability gate:

```bash
for run in 1 2 3; do
  env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
    PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
    mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
    -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
    -Dtest=IndexSourceRemoteRecoveryITCase test || exit 1
done
```

Expected: 3/3 passes.

Neighboring suite:

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home \
  PATH=/Library/Java/JavaVirtualMachines/microsoft-17.jdk/Contents/Home/bin:$PATH \
  mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=IndexSourceRemoteRecoveryITCase,KvRecoverFromRemoteLogITCase,IndexPushOrderingITCase,IndexTargetRecoveryITCase test
```

Expected: 4/4 passes.

Static checks:

```bash
rg -n 'Thread\.sleep|TimeUnit\.[A-Z]+\.sleep|createFixture\(|private .* fixture' \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceRemoteRecoveryITCase.java
git diff --check
```

Expected: both commands print no findings.

- [ ] **Step 6: Mark the design implemented and commit**

Change the design status to:

```markdown
The design was implemented and verified on 2026-07-15.
```

Then commit the cohesive phase refactor and status update:

```bash
git add \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceRemoteRecoveryITCase.java \
  docs/superpowers/specs/2026-07-15-index-source-remote-recovery-test-maintainability-design.md
git commit -m "Clarify remote recovery test phases"
```

---

## Final Review Gate

After both tasks, request an independent review over the two implementation commits. The reviewer must verify that no assertion, topology condition, remote endpoint gate, cleanup guarantee, or physical projection check was lost. Resolve every Critical or Important finding before declaring the refactor complete; record Minor findings without broadening this test-only scope.
