# Retract Architecture Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor retract support from paired-record protocol to per-record MutationType model, eliminating implicit pairing, checkpoint liveness risk, and MergeMode forward-compatibility issues.

**Architecture:** Each KvRecord in PUT_KV V2 carries a 1-byte MutationType (UPSERT/DELETE/RETRACT). Server processes each record independently with best-effort merge optimization for adjacent retract+upsert pairs. Flink sends -U and +U as independent records without buffering.

**Tech Stack:** Java 11, Apache Fluss (fluss-common, fluss-client, fluss-server, fluss-flink), Protobuf, Maven

**Spec:** `docs/superpowers/specs/2026-03-27-retract-architecture-redesign.md`

**Build command:** `./mvnw clean package -pl <module> -am -DskipTests -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`

**Test command:** `./mvnw test -pl <module> -Dtest=<TestClass> -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`

**Format command:** `./mvnw spotless:apply -pl <module> -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`

---

## File Structure

### New files
- `fluss-common/src/main/java/org/apache/fluss/record/MutationType.java` — per-record mutation type enum
- `fluss-common/src/test/java/org/apache/fluss/record/MutationTypeTest.java` — enum tests

### Modified files (by task order)

**Task 1 — MergeMode rollback:**
- `fluss-common/src/main/java/org/apache/fluss/rpc/protocol/MergeMode.java` — remove RETRACT_THEN_AGGREGATE
- `fluss-common/src/test/java/org/apache/fluss/rpc/protocol/MergeModeTest.java` — update tests
- `fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/ApiKeys.java` — rewrite PUT_KV V2 comment
- `fluss-rpc/src/main/proto/FlussApi.proto` — remove RETRACT_THEN_AGGREGATE comments

**Task 2 — MutationType enum:**
- `fluss-common/src/main/java/org/apache/fluss/record/MutationType.java` — new file
- `fluss-common/src/test/java/org/apache/fluss/record/MutationTypeTest.java` — new file

**Task 3 — KvRecord V2 wire format:**
- `fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecord.java` — add writeToV2/readFromV2
- `fluss-common/src/main/java/org/apache/fluss/record/KvRecord.java` — add getMutationType() default
- `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatchBuilder.java` — add V2 append, remove hasRoomForPair
- `fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecordBatch.java` — V2 iterator support
- `fluss-common/src/main/java/org/apache/fluss/record/KvRecordReadContext.java` — add isV2Format flag
- `fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordTest.java` — V2 round-trip tests
- `fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordBatchTest.java` — V2 batch tests

**Task 4 — Client layer refactor:**
- `fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java` — add mutationType field, forRetract()
- `fluss-client/src/main/java/org/apache/fluss/client/write/KvWriteBatch.java` — remove tryAppendPair, pass mutationType to builder
- `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java` — remove appendPair/appendNewBatchPair
- `fluss-client/src/main/java/org/apache/fluss/client/write/WriterClient.java` — remove sendPair/doSendPair
- `fluss-client/src/main/java/org/apache/fluss/client/table/writer/AbstractTableWriter.java` — remove sendPairWithResult
- `fluss-client/src/main/java/org/apache/fluss/client/table/writer/UpsertWriter.java` — replace retractThenUpsert with retract
- `fluss-client/src/main/java/org/apache/fluss/client/table/writer/UpsertWriterImpl.java` — implement retract(), remove retractThenUpsert()
- `fluss-client/src/test/java/org/apache/fluss/client/write/WriteRecordTest.java` — update tests
- `fluss-client/src/test/java/org/apache/fluss/client/write/KvWriteBatchTest.java` — update tests

**Task 5 — Server RETRACT processing:**
- `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java` — rewrite processKvRecords with MutationType dispatch
- `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java` — pass API version to putAsLeader
- `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java` — thread apiVersion through putToLocalKv
- `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java` — accept and forward apiVersion to KvTablet.putAsLeader
- `fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletMergeModeTest.java` — rewrite retract tests
- `fluss-server/src/test/java/org/apache/fluss/server/kv/rowmerger/AggregateRowMergerTest.java` — retract tests unchanged

**Task 6 — Flink layer simplification:**
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/sink/writer/UpsertSinkWriter.java` — remove pendingRetractRows, simplify writeRow
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/row/OperationType.java` — update RETRACT Javadoc (remove MergeMode#RETRACT_THEN_AGGREGATE reference)
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/sink/FlinkTableSink.java` — unchanged (getChangelogMode stays)
- `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/sink/writer/UpsertSinkWriterTest.java` — rewrite retract tests
- `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/row/RowWithOpTest.java` — unchanged
- `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/utils/TestUpsertWriter.java` — replace retractThenUpsert with retract

**Task 7 — Integration test:**
- `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/sink/FlinkTableSinkITCase.java` — update E2E retract test

---

## Tasks

### Task 1: MergeMode Rollback

Remove `RETRACT_THEN_AGGREGATE` from `MergeMode` enum and all references. This is a pure deletion task that unblocks all subsequent tasks.

**Files:**
- Modify: `fluss-common/src/main/java/org/apache/fluss/rpc/protocol/MergeMode.java`
- Modify: `fluss-common/src/test/java/org/apache/fluss/rpc/protocol/MergeModeTest.java`
- Modify: `fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/ApiKeys.java` — rewrite PUT_KV V2 comment
- Modify: `fluss-rpc/src/main/proto/FlussApi.proto` — remove RETRACT_THEN_AGGREGATE comments

**Context:** The current `retract` branch added `RETRACT_THEN_AGGREGATE(2)` to `MergeMode`. This value is used throughout the client/server pipeline. In the new design, retract is expressed via per-record `MutationType`, so `RETRACT_THEN_AGGREGATE` must be removed. `MergeMode` retains only `DEFAULT(0)` and `OVERWRITE(1)`.

- [ ] **Step 1: Update MergeMode.java**

Remove the `RETRACT_THEN_AGGREGATE` enum value (line 78) and its cases in `fromValue()` (line 122-123) and `fromProtoValue()`. The enum should only have `DEFAULT` and `OVERWRITE`.

Update the `ApiKeys.java` PUT_KV version comment (lines 51-53) to describe V2 as per-record MutationType format instead of request-level merge mode.

Update `FlussApi.proto` to remove `RETRACT_THEN_AGGREGATE` comments (lines 241, 255).

- [ ] **Step 2: Update MergeModeTest.java**

Remove all test cases referencing `RETRACT_THEN_AGGREGATE`. Update `testValuesSize` assertion to expect 2 values instead of 3.

- [ ] **Step 3: Compile and verify**

Run: `./mvnw clean compile -pl fluss-common -am -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`

This will fail with compilation errors in files that reference `RETRACT_THEN_AGGREGATE`. That is expected — those files will be fixed in subsequent tasks. For now, just verify that `MergeMode.java` and `MergeModeTest.java` themselves compile.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "refactor: remove RETRACT_THEN_AGGREGATE from MergeMode enum"
```

---

### Task 2: Create MutationType Enum

New per-record mutation type enum that replaces the batch-level MergeMode for retract semantics.

**Files:**
- Create: `fluss-common/src/main/java/org/apache/fluss/record/MutationType.java`
- Create: `fluss-common/src/test/java/org/apache/fluss/record/MutationTypeTest.java`

- [ ] **Step 1: Write MutationTypeTest.java**

```java
package org.apache.fluss.record;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MutationTypeTest {

    @Test
    void testEnumValues() {
        assertThat(MutationType.UPSERT.getValue()).isEqualTo((byte) 0);
        assertThat(MutationType.DELETE.getValue()).isEqualTo((byte) 1);
        assertThat(MutationType.RETRACT.getValue()).isEqualTo((byte) 2);
    }

    @Test
    void testFromValue() {
        assertThat(MutationType.fromValue((byte) 0)).isEqualTo(MutationType.UPSERT);
        assertThat(MutationType.fromValue((byte) 1)).isEqualTo(MutationType.DELETE);
        assertThat(MutationType.fromValue((byte) 2)).isEqualTo(MutationType.RETRACT);
    }

    @Test
    void testFromValueUnknownThrows() {
        assertThatThrownBy(() -> MutationType.fromValue((byte) 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown MutationType");
    }

    @Test
    void testValuesCount() {
        assertThat(MutationType.values()).hasSize(3);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw test -pl fluss-common -Dtest=MutationTypeTest -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`
Expected: FAIL — `MutationType` class does not exist.

- [ ] **Step 3: Write MutationType.java**

```java
package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * The mutation type of a {@link KvRecord} in PUT_KV V2 format.
 *
 * <p>This is a per-record field inside the KvRecord body, independent of the
 * batch/request-level {@link org.apache.fluss.rpc.protocol.MergeMode}.
 *
 * @since 0.10
 */
@PublicEvolving
public enum MutationType {
    /** Normal upsert record. Row must not be null. */
    UPSERT((byte) 0),

    /** Explicit delete record. Row must be null. */
    DELETE((byte) 1),

    /** Retract record carrying the old value to be retracted. Row must not be null. */
    RETRACT((byte) 2);

    private final byte value;

    MutationType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static MutationType fromValue(byte value) {
        switch (value) {
            case 0:
                return UPSERT;
            case 1:
                return DELETE;
            case 2:
                return RETRACT;
            default:
                throw new IllegalArgumentException("Unknown MutationType value: " + value);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw test -pl fluss-common -Dtest=MutationTypeTest -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`
Expected: PASS

- [ ] **Step 5: Format and commit**

```bash
./mvnw spotless:apply -pl fluss-common -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
git add -A && git commit -m "feat: add MutationType enum for per-record mutation identification"
```

---

### Task 3: KvRecord V2 Wire Format

Add V2 read/write support to `DefaultKvRecord` with per-record MutationType byte. Add `getMutationType()` to `KvRecord` interface. Update `KvRecordBatchBuilder` for V2 append. Update `DefaultKvRecordBatch` iterator for V2 parsing.

**Files:**
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecord.java` — add getMutationType() default
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecord.java` — add writeToV2, readFromV2, sizeOfV2
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatch.java` — add isV2Format to ReadContext
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordReadContext.java` — accept and expose v2Format flag
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatchBuilder.java` — add V2 append, remove hasRoomForPair
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecordBatch.java` — V2 iterator
- Modify: `fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordTest.java` — V2 round-trip tests
- Modify: `fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordBatchTest.java` — V2 batch tests

**Context:** V0/V1 record format is `Length(4B) + KeyLength(varint) + Key + Row`. V2 adds `MutationType(1B)` after Length: `Length(4B) + MutationType(1B) + KeyLength(varint) + Key + Row`. The `ReadContext` must carry a flag indicating V2 format so the iterator knows which parser to use.

- [ ] **Step 1: Add getMutationType() default to KvRecord interface**

In `KvRecord.java`, add:
```java
default MutationType getMutationType() {
    return getRow() == null ? MutationType.DELETE : MutationType.UPSERT;
}
```

- [ ] **Step 2: Add V2 write/read/size methods to DefaultKvRecord**

Add `writeToV2(OutputView, MutationType, byte[], BinaryRow)` — writes MutationType byte before key.
Add `readFromV2(MemorySegment, int, short, ReadContext)` — reads MutationType byte, stores it, overrides `getMutationType()`.
Add `sizeOfV2(byte[], BinaryRow)` — returns `sizeOf(key, row) + 1` (1 byte for MutationType).

The V2 `DefaultKvRecord` instance must store the parsed `MutationType` and override `getMutationType()` to return it instead of the default null-row heuristic.

- [ ] **Step 3: Add isV2Format to ReadContext**

In `KvRecordBatch.ReadContext`, add:
```java
default boolean isV2Format() { return false; }
```

Update `KvRecordReadContext` (the concrete implementation used by server) to accept and expose a `v2Format` flag.

- [ ] **Step 4: Update DefaultKvRecordBatch iterator**

In the `KvRecordIterator.readNext()` method, check `readContext.isV2Format()`:
- If true: call `DefaultKvRecord.readFromV2(...)`
- If false: call `DefaultKvRecord.readFrom(...)` (existing)

- [ ] **Step 5: Update KvRecordBatchBuilder**

Remove `hasRoomForPair()` method.
Add `appendV2(MutationType, byte[], BinaryRow)` method that writes the MutationType byte before delegating to the existing key+row serialization.
Add `hasRoomForV2(byte[], BinaryRow)` that accounts for the extra 1 byte.

- [ ] **Step 6: Write V2 round-trip tests in DefaultKvRecordTest**

Test cases:
- Write V2 UPSERT record, read back, verify getMutationType()==UPSERT and row matches
- Write V2 DELETE record (null row), read back, verify getMutationType()==DELETE
- Write V2 RETRACT record, read back, verify getMutationType()==RETRACT and row matches
- Write V2 UPSERT with null row → verify sizeOfV2 calculation is correct
- Verify V0 record read still returns default getMutationType() (UPSERT for non-null, DELETE for null)

- [ ] **Step 7: Write V2 batch tests in DefaultKvRecordBatchTest**

Test cases:
- Build a V2 batch with mixed UPSERT/DELETE/RETRACT records, iterate, verify each record's MutationType
- Build a V0 batch, iterate with V0 context, verify default MutationType behavior
- Verify record count and size calculations for V2 batches

- [ ] **Step 8: Run all tests**

Run: `./mvnw test -pl fluss-common -Dtest=DefaultKvRecordTest,DefaultKvRecordBatchTest,MutationTypeTest -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`
Expected: PASS

- [ ] **Step 9: Format and commit**

```bash
./mvnw spotless:apply -pl fluss-common -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
git add -A && git commit -m "feat: add KvRecord V2 wire format with per-record MutationType"
```

---

### Task 4: Client Layer Refactor

Add `mutationType` field to `WriteRecord`, add `forRetract()` factory, replace `retractThenUpsert()` with `retract()` on `UpsertWriter`, remove all pair-related code.

**Files:**
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/write/KvWriteBatch.java`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/write/WriterClient.java`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/table/writer/AbstractTableWriter.java`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/table/writer/UpsertWriter.java`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/table/writer/UpsertWriterImpl.java`
- Modify: `fluss-client/src/test/java/org/apache/fluss/client/write/WriteRecordTest.java`
- Modify: `fluss-client/src/test/java/org/apache/fluss/client/write/KvWriteBatchTest.java`

- [ ] **Step 1: Add mutationType to WriteRecord**

Add `private final MutationType mutationType;` field. Add it to existing constructors (default to `MutationType.UPSERT` for `forUpsert()`, `MutationType.DELETE` for `forDelete()`). Add `WriteRecord.forRetract(...)` factory method that sets `mutationType=MutationType.RETRACT`. Add `getMutationType()` getter.

Remove `RETRACT_THEN_AGGREGATE` references from any `forUpsert(..., MergeMode)` overloads — the `mergeMode` parameter stays but only accepts DEFAULT/OVERWRITE.

- [ ] **Step 2: Update KvWriteBatch**

Remove `tryAppendPair()` method. In `tryAppend()`, pass `writeRecord.getMutationType()` through to the builder's append method (for V2 format). Remove the `mergeMode` consistency check for `RETRACT_THEN_AGGREGATE` in `validateRecordConsistency()` (the check for DEFAULT/OVERWRITE consistency stays).

- [ ] **Step 3: Remove pair code from RecordAccumulator**

Delete `appendPair()`, `appendNewBatchPair()`, and any helper methods only used by pair logic. The `append()` method stays unchanged — retract records flow through the same path as upsert records.

- [ ] **Step 4: Remove pair code from WriterClient**

Delete `sendPair()` and `doSendPair()`. The `send()` method stays unchanged.

- [ ] **Step 5: Update UpsertWriter interface and implementation**

In `UpsertWriter.java`: remove `retractThenUpsert(InternalRow, InternalRow)`, add `retract(InternalRow row)`.

In `UpsertWriterImpl.java`: implement `retract(InternalRow row)` — encode the row, create `WriteRecord.forRetract(...)`, send via `writerClient.send()`. Remove `retractThenUpsert()` implementation.

In `AbstractTableWriter.java`: remove `sendPairWithResult()`.

- [ ] **Step 6: Update WriteRecordTest**

Add test for `forRetract()` factory: verify `getMutationType()==RETRACT`, verify key/value encoding.
Update existing tests that referenced `RETRACT_THEN_AGGREGATE` merge mode.

- [ ] **Step 7: Update KvWriteBatchTest**

Remove `testMergeModeConsistencyValidation` cases for `RETRACT_THEN_AGGREGATE`.
Remove any `tryAppendPair` tests.
Add test: batch with mixed UPSERT and RETRACT records via `tryAppend()` succeeds.

- [ ] **Step 8: Compile and run tests**

Run: `./mvnw test -pl fluss-client -Dtest=WriteRecordTest,KvWriteBatchTest -am -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`
Expected: PASS

- [ ] **Step 9: Format and commit**

```bash
./mvnw spotless:apply -pl fluss-client -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
git add -A && git commit -m "refactor: replace pair-based retract with per-record MutationType in client"
```

---

### Task 5: Server RETRACT Processing

Rewrite `KvTablet.processKvRecords()` to dispatch on `MutationType` from each record. Add RETRACT case with independent processing and best-effort merge optimization. Remove old `processRetractThenAggregateRecords()` and `processRetractThenAggregate()`. Thread API version from `TabletService` to record parsing.

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java` — thread apiVersion through putToLocalKv
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java` — accept and forward apiVersion to KvTablet.putAsLeader
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletMergeModeTest.java`

**Context:** The current `processKvRecords()` checks `if (mergeMode == RETRACT_THEN_AGGREGATE)` and delegates to a separate method. In the new design, the main loop reads `record.getMutationType()` and handles RETRACT inline. The API version must be threaded from `TabletService.putKv()` through `ReplicaManager` to `KvTablet.putAsLeader()` so the `ReadContext` knows whether to parse V2 format.

- [ ] **Step 1: Thread API version to KvTablet**

In `TabletService.putKv()`: extract API version from the request context. Pass it through `ReplicaManager.putRecordsToKv()` → `ReplicaManager.putToLocalKv()` → `Replica.putRecordsToLeader()` → `KvTablet.putAsLeader()`. Each method in the chain needs an `apiVersion` parameter added. In `putAsLeader()`, pass it to `processKvRecords()`. In `processKvRecords()`, construct `KvRecordReadContext` with `isV2Format = (apiVersion >= 2)`.

- [ ] **Step 2: Rewrite processKvRecords() RETRACT handling**

In the main record iteration loop, after reading each `KvRecord`, check `record.getMutationType()`:

- `UPSERT`: existing `dispatchMutation()` path (unchanged)
- `DELETE`: existing `dispatchMutation()` path (unchanged, null row = delete)
- `RETRACT`: new inline handling per spec Section 2.3:
  1. Validate `currentMerger.supportsRetract()`, throw `InvalidRecordException` if false
  2. Read old value from pre-write buffer or KV store
  3. If old value is null, skip (retract on non-existent key)
  4. Peek next record: if same key and UPSERT, consume it and do merged retract+upsert
  5. Otherwise: independent retract with delete behavior handling

- [ ] **Step 3: Delete old retract methods**

Remove `processRetractThenAggregateRecords()` and `processRetractThenAggregate()` entirely. Remove the `if (mergeMode == MergeMode.RETRACT_THEN_AGGREGATE)` branch in `processKvRecords()`.

- [ ] **Step 4: Rewrite KvTabletMergeModeTest retract tests**

The existing tests use `MergeMode.RETRACT_THEN_AGGREGATE` at the batch level. Rewrite them to:
- Build V2 format batches with `MutationType.RETRACT` and `MutationType.UPSERT` records
- Test: paired retract+upsert on existing key → verify UB(old)+UA(new) CDC, 2 entries
- Test: unpaired retract on existing key → verify UB(old)+UA(intermediate) CDC
- Test: retract on non-existent key → verify no CDC
- Test: retract on non-aggregation table → verify InvalidRecordException
- Test: retract on aggregation table with non-retractable function → verify InvalidRecordException
- Test: mixed UPSERT and RETRACT records in same batch → verify correct processing
- Test: multiple paired retract+upsert for same key in one batch → verify cumulative effect
- Test: retract to zero then rescue (retract reduces to 0, next upsert restores) → verify correctness

- [ ] **Step 5: Run tests**

Run: `./mvnw test -pl fluss-server -Dtest=KvTabletMergeModeTest -am -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`
Expected: PASS

- [ ] **Step 6: Format and commit**

```bash
./mvnw spotless:apply -pl fluss-server -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
git add -A && git commit -m "feat: server-side per-record MutationType dispatch with independent retract processing"
```

---

### Task 6: Flink Layer Simplification

Remove `pendingRetractRows` buffering from `UpsertSinkWriter`. RETRACT operations are sent directly without buffering. Remove all checkpoint-related retract logic.

**Files:**
- Modify: `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/sink/writer/UpsertSinkWriter.java`
- Modify: `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/row/OperationType.java` — update RETRACT Javadoc
- Modify: `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/sink/writer/UpsertSinkWriterTest.java`
- Modify: `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/utils/TestUpsertWriter.java`

- [ ] **Step 1: Simplify UpsertSinkWriter.writeRow()**

Remove `pendingRetractRows` field and its initialization in `open()`.

Rewrite `writeRow()`:
```java
case RETRACT:
    if (!schemaSupportsRetract) {
        throw new UnsupportedOperationException("Schema does not support retract");
    }
    CompletableFuture<Void> retractFuture = upsertWriter.retract(internalRow);
    if (offsetReporter != null) {
        retractFuture.thenAccept(reportOffsetIfAvailable);
    }
    break;
case UPSERT:
    // existing upsert logic, but remove the pendingRetractRows check
    CompletableFuture<Void> upsertFuture = upsertWriter.upsert(internalRow);
    if (offsetReporter != null) {
        upsertFuture.thenAccept(reportOffsetIfAvailable);
    }
    break;
```

Remove from `flush()`: the `pendingRetractRows.isEmpty()` check and `IOException`.
Remove from `close()`: the orphaned retract warning logic.

- [ ] **Step 2: Update OperationType.java Javadoc**

In `OperationType.java`, update the `RETRACT` Javadoc to remove the reference to `MergeMode#RETRACT_THEN_AGGREGATE`. Replace with a description referencing the per-record `MutationType.RETRACT` semantics.

- [ ] **Step 3: Update TestUpsertWriter**

Replace `retractThenUpsert(InternalRow, InternalRow)` with `retract(InternalRow)`. The `retract()` method should increment a counter and return a completed future.

- [ ] **Step 4: Rewrite UpsertSinkWriterTest**

Remove tests that depend on pairing logic:
- `testInterleavedRetractsArePairedByLogicalPrimaryKey` → replace with `testRetractSentDirectlyWithoutBuffering`
- `testDuplicateRetractForSameKeyFailsFast` → remove (no longer applicable, no buffering)
- `testFlushFailsWithUnmatchedRetract` → remove (no longer applicable, no pending retracts)
- `testRetractRowIsDeepCopiedAcrossReusedFlinkRowWrapper` → keep if deep-copy is still needed for retract

New tests:
- `testRetractSentDirectlyWithoutBuffering`: send RETRACT, verify `upsertWriter.retract()` called immediately
- `testRetractOnNonRetractSchemaThrows`: send RETRACT with `schemaSupportsRetract=false`, verify UnsupportedOperationException
- `testMixedRetractAndUpsertInSequence`: send RETRACT then UPSERT for same key, verify both sent independently

- [ ] **Step 5: Run tests**

Run: `./mvnw test -pl fluss-flink/fluss-flink-common -Dtest=UpsertSinkWriterTest -am -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository`
Expected: PASS

- [ ] **Step 6: Format and commit**

```bash
./mvnw spotless:apply -pl fluss-flink/fluss-flink-common -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
git add -A && git commit -m "refactor: remove retract buffering from UpsertSinkWriter, send retract directly"
```

---

### Task 7: Integration Test and Cleanup

Update the E2E integration test to verify the new retract architecture works end-to-end. Fix any remaining compilation errors from the MergeMode rollback. Run full test suite.

**Files:**
- Modify: `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/sink/FlinkTableSinkITCase.java`
- Modify: any remaining files with `RETRACT_THEN_AGGREGATE` references

- [ ] **Step 1: Find and fix remaining RETRACT_THEN_AGGREGATE references**

Search the entire codebase for `RETRACT_THEN_AGGREGATE` and fix any remaining references:
```bash
grep -rn "RETRACT_THEN_AGGREGATE" fluss-*/src/
```

Common locations to check:
- `Sender.java` / `SenderTest.java`
- `UndoComputerTest.java`
- `RowWithOpTest.java`
- `FlinkConversionsTest.java`
- Any test helper classes

- [ ] **Step 2: Update FlinkTableSinkITCase**

The existing `testRetractOnAggregationTableEndToEnd()` should continue to work with the new architecture — the Flink SQL pipeline produces UPDATE_BEFORE/UPDATE_AFTER which now flows through the simplified path. Verify the test still passes without modification, or update if the test setup referenced `RETRACT_THEN_AGGREGATE`.

- [ ] **Step 3: Run full module tests**

Run each module's tests to verify no regressions:

```bash
./mvnw test -pl fluss-common -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
./mvnw test -pl fluss-client -am -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
./mvnw test -pl fluss-server -am -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
./mvnw test -pl fluss-flink/fluss-flink-common -am -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

- [ ] **Step 4: Format all and final commit**

```bash
./mvnw spotless:apply -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
git add -A && git commit -m "test: update integration tests and fix remaining RETRACT_THEN_AGGREGATE references"
```
