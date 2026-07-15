# Index Source Remote Recovery Test Maintainability Design

## Status

The written design was approved on 2026-07-15 and is ready for implementation planning.

## Goal

Make `IndexSourceRemoteRecoveryITCase` easier to audit and modify without weakening its causal
proof of snapshot-based index progress, raw remote source-WAL replay, target acknowledgement
ordering, source failover, exact physical Index Table projection, and local continuation.

## Non-Goals

- No production-code change.
- No change to cluster topology, workload, timeouts, gates, or assertions.
- No split into multiple independent test cases.
- No shared failover DSL, common test base, or broadly reusable abstraction.
- No line-count target and no removal of failure-only diagnostics.

## Current Problems

The test method currently owns orchestration, mutable cleanup state, offsets, expected rows, target
acknowledgement gating, failover, and final evidence logging. Its chronology is correct but costly
to review. The class-level `fixture` field also gives helper methods hidden dependencies.

`createFixture()` combines table creation, readiness waits, runtime role discovery, topology
validation, target-bucket selection, and test-value construction. Its generic name understates both
its cost and its behavior.

The remote-log diagnostics and physical Index Table oracle are valuable, but their size obscures
the test's main recovery protocol when navigating the file.

## Design

### Preserve One Causal Test

Keep the existing single `@Test`. Express its chronology through five phase methods:

1. Persist and verify the exact baseline snapshot.
2. Create a committed source-WAL range that the retired replicator has not pushed.
3. Wait until raw remote log covers the complete replay range.
4. Promote the selected follower, hold the target acknowledgement, and prove remote replay occurs
   before index progress advances.
5. Release the acknowledgement, verify the complete physical projection, and prove local
   continuation.

Each phase keeps its assertions adjacent to the action that establishes the invariant. The top-level
test remains the readable protocol outline; phase extraction must not turn it into assertion-free
ceremony.

### Explicit Test State

Remove the class-level `fixture` field. Introduce `RemoteRecoveryFixture`, passed explicitly to
phase and helper methods. It owns:

- immutable table identifiers and discovered TabletServer roles;
- the source and gated target buckets;
- the two bucket-directed index values;
- the initial source replica;
- the set of TabletServers stopped by this test; and
- the optional replay gate registration.

The top-level setup reads:

```java
try (RemoteRecoveryFixture recovery = setUpRemoteRecovery()) {
    // Execute the five causal phases.
}
```

`setUpRemoteRecovery()` is intentionally named as active test setup rather than object allocation:
it creates metadata, waits for replicas, discovers runtime assignments, and validates the required
four-role topology.

Its implementation delegates the independently understandable operations to:

- `createIndexedSourceTable()`;
- `resolveRecoveryRoles(...)`; and
- `findTargetBucketLedBy(...)`.

These helpers expose the expensive behavior without introducing a general topology framework.

### Resource Ownership

`RemoteRecoveryFixture` implements `AutoCloseable`. Server stop/start operations and replay-gate
installation go through the fixture so it can restore every resource it changed.

`close()` releases and unregisters the replay gate, restarts all still-stopped TabletServers, and
verifies that the cluster returns to its configured TabletServer count. It aggregates cleanup
failures. Try-with-resources preserves a cleanup failure as suppressed when the test already has a
primary failure.

This replaces the top-level `primaryFailure`, `cleanupFailures`, and nullable local gate state while
preserving their current failure semantics.

### Cohesive Helpers

Keep remote-log coverage diagnostics and physical projection verification in this test file. Group
them as private nested helpers only when doing so reduces navigation and parameter noise:

- remote-log state formatting and coverage waits remain failure-oriented and must not perform
  expensive diagnostics in the polling hot path;
- physical key/value encoding, RocksDB scanning, exact projection comparison, and absence checks
  remain one oracle over the real Index Table bytes.

Do not move either helper into production code or a shared test utility without a second concrete
consumer.

## Verification

The refactor is accepted only when all of the following remain true:

- the focused test passes once;
- the focused test passes three consecutive times;
- the neighboring remote recovery and index ordering suite passes together;
- the test still proves both replay endpoints are unavailable locally and available remotely;
- target acknowledgement remains held while remote-read bytes increase and pushed offset remains
  at the persisted baseline;
- final verification compares every physical Index Table key and value and checks stale and
  never-written keys are absent;
- post-recovery local writes advance the exact pushed offset and physical projection;
- no fixed sleep is introduced; and
- `git diff --check` is clean.

The refactor should be judged by clearer ownership and causality, not by total file length alone.
