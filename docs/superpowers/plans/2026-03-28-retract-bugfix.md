# Retract Architecture Bugfix Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 3 confirmed bugs in the retract architecture commit (ca778324): V2 format dead code, logOffset over-counting, and V2 size estimation.

**Architecture:** Batch self-describes its record format via attributes bit 0 (V0 vs V2). Server reads format from batch header instead of API version. Client decides V2 per-batch based on whether the first record is RETRACT. logOffset strictly tracks CDC output count.

**Tech Stack:** Java 11, Fluss record format, KvRecordBatch wire protocol

**Build command (always use):**
```bash
cd /Users/wangyang/Desktop/merge-engine/fluss
./mvnw <goal> -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

---

## File Map

| File | Change | Responsibility |
|------|--------|---------------|
| `fluss-common/.../record/DefaultKvRecordBatch.java` | Modify | Add `isV2Format()` reading attributes bit 0 |
| `fluss-common/.../record/KvRecordBatch.java` | Modify | Add `isV2Format()` to interface |
| `fluss-common/.../record/KvRecordBatchBuilder.java` | Modify | Accept v2Format flag, write attributes bit 0 |
| `fluss-server/.../kv/KvTablet.java` | Modify | Use batch attributes instead of apiVersion; fix logOffset |
| `fluss-client/.../write/RecordAccumulator.java` | Modify | Remove v2Format field; decide per-batch from MutationType |
| `fluss-client/.../write/KvWriteBatch.java` | Modify | Reject RETRACT in V0 batch |
| `fluss-client/.../write/WriteRecord.java` | Modify | RETRACT uses `sizeOfV2()` |
| `fluss-common/...test.../DefaultKvRecordBatchTest.java` | Modify | Test attributes bit round-trip |
| `fluss-client/...test.../KvWriteBatchTest.java` | Modify | Test V0 rejects RETRACT, V2 accepts mixed |
| `fluss-server/...test.../KvTabletTest.java` | Modify | Test logOffset correctness for skip branches |

---

### Task 1: Batch Attributes — V2 Format Self-Description

Make the batch header's attributes byte carry the V2 format flag so server can determine record format from the batch itself.

**Files:**
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecordBatch.java:55-62,81`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatch.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatchBuilder.java:45-86,248-252`
- Test: `fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordBatchTest.java`

- [ ] **Step 1: Add attributes constant to DefaultKvRecordBatch**

In `DefaultKvRecordBatch.java`, add after the existing attribute doc comment (line 55-61):

```java
/** Bit mask for V2 record format in the attributes byte. */
static final byte V2_FORMAT_ATTRIBUTE_MASK = 0x01;
```

And add a method to read it:

```java
/** Whether records in this batch use V2 wire format (with per-record MutationType byte). */
public boolean isV2Format() {
    return (segment.get(position + ATTRIBUTES_OFFSET) & V2_FORMAT_ATTRIBUTE_MASK) != 0;
}
```

- [ ] **Step 2: Add `isV2Format()` to KvRecordBatch interface**

In `KvRecordBatch.java`, add after `getRecordCount()`:

```java
/**
 * Whether records in this batch use V2 wire format (with per-record MutationType byte).
 * V2 batches have attributes bit 0 set.
 */
default boolean isV2Format() {
    return false;
}
```

- [ ] **Step 3: Make KvRecordBatchBuilder accept v2Format**

In `KvRecordBatchBuilder.java`:

Add field:
```java
private final boolean v2Format;
```

Update private constructor to accept `boolean v2Format` parameter and store it.

Keep the existing 4-arg `builder()` factory as a backward-compatible overload defaulting to `v2Format=false` (this avoids breaking 6 existing call sites in tests). Add a new 5-arg overload:

```java
public static KvRecordBatchBuilder builder(
        int schemaId, int writeLimit, AbstractPagedOutputView outputView, KvFormat kvFormat) {
    return builder(schemaId, writeLimit, outputView, kvFormat, false);
}

public static KvRecordBatchBuilder builder(
        int schemaId, int writeLimit, AbstractPagedOutputView outputView, KvFormat kvFormat,
        boolean v2Format) {
    return new KvRecordBatchBuilder(
            schemaId, CURRENT_KV_MAGIC_VALUE, writeLimit, outputView, kvFormat, v2Format);
}
```

Update `computeAttributes()`:
```java
private byte computeAttributes() {
    return v2Format ? DefaultKvRecordBatch.V2_FORMAT_ATTRIBUTE_MASK : 0;
}
```

- [ ] **Step 4: Update KvRecordTestUtils.ofV2Records() to use v2Format=true**

In `fluss-common/src/test/java/org/apache/fluss/record/KvRecordTestUtils.java`, the `ofV2Records()` method (line 102) builds V2 format batches but currently uses the 4-arg `builder()`. Update it to use the 5-arg overload with `v2Format=true`, so the batch attributes correctly reflect V2 format:

```java
KvRecordBatchBuilder.builder(schemaId, writeLimit, outputView, kvFormat, true)
```

The `ofRecords()` method (line 70) keeps using the 4-arg overload (V0 format) — no change needed.

- [ ] **Step 5: Write test for attributes round-trip**

In `DefaultKvRecordBatchTest.java`, add a test that:
1. Builds a V2 batch with `KvRecordBatchBuilder.builder(schemaId, writeLimit, outputView, kvFormat, true)`
2. Appends a record via `appendV2(MutationType.UPSERT, key, row)`
3. Builds and reads back via `DefaultKvRecordBatch`
4. Asserts `batch.isV2Format() == true`
5. Also test V0 batch: `isV2Format() == false`

- [ ] **Step 6: Run test**

```bash
./mvnw test -pl fluss-common -Dtest=DefaultKvRecordBatchTest -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "fix(record): batch attributes bit 0 carries V2 format flag"
```

---

### Task 2: Server — Read Format from Batch Attributes, Not API Version

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java:495-500`

- [ ] **Step 1: Replace apiVersion-based V2 detection with batch attributes**

In `KvTablet.processKvRecords()`, replace:

```java
boolean isV2 = apiVersion >= 2;
KvRecordBatch.ReadContext readContext =
        KvRecordReadContext.createReadContext(kvFormat, schemaGetter, isV2);
```

With:

```java
boolean isV2 = kvRecords.isV2Format();
KvRecordBatch.ReadContext readContext =
        KvRecordReadContext.createReadContext(kvFormat, schemaGetter, isV2);
```

The `apiVersion` parameter to `processKvRecords()` can be kept for now (other validation may use it), but V2 record parsing is now driven by batch self-description.

- [ ] **Step 2: Run existing server tests**

```bash
./mvnw test -pl fluss-server -Dtest=KvTabletTest -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "fix(server): read V2 format from batch attributes instead of API version"
```

---

### Task 3: Server — Fix logOffset Accounting in RETRACT Branches

All skip branches must follow the existing convention: no WAL write → no logOffset increment. Merged pair must not add extra +1 for "second input record".

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java:567-569,596-629,688-703`

- [ ] **Step 1: Fix missing-key retract skip**

At line 567-570, replace:

```java
if (oldValueBytes == null) {
    // Retract on non-existent key — skip silently
    logOffset++;
    break;
}
```

With:

```java
if (oldValueBytes == null) {
    // Retract on non-existent key — skip silently (no CDC output, no offset consumed)
    break;
}
```

- [ ] **Step 2: Fix merged pair skip branches and remove extra logOffset++**

In the merged retract+upsert block (lines 596-629):

Replace `logOffset += 2` at line 599 (IGNORE branch) with: remove the line (just `break` out of the if).

```java
if (deleteBehavior == DeleteBehavior.IGNORE) {
    // no CDC output, no offset consumed
```

Replace `logOffset += 2` at line 618 (no-change branch) with: remove the line.

```java
else if (newValue.equals(oldValue)) {
    // no net change, skip (no CDC output, no offset consumed)
```

Remove `logOffset++` at line 611 (after `applyDelete` in merged pair):

```java
logOffset =
        applyDelete(
                mutation.key,
                oldValue,
                walBuilder,
                latestSchemaRow,
                logOffset);
// DELETE: logOffset already advanced by applyDelete
```

Remove `logOffset++` at line 628 (after `applyUpdate` in merged pair):

```java
logOffset =
        applyUpdate(
                mutation.key,
                oldValue,
                newValue,
                walBuilder,
                latestSchemaRow,
                logOffset);
// UPDATE: logOffset already advanced by applyUpdate
```

- [ ] **Step 3: Fix independent retract skip branches**

In `processIndependentRetract()` (lines 688-703):

Replace `return logOffset + 1` at line 692 (IGNORE) with:

```java
return logOffset; // no CDC output, no offset consumed
```

Replace `return logOffset + 1` at line 703 (no-change) with:

```java
return logOffset; // no change, no offset consumed
```

- [ ] **Step 4: Run server tests**

```bash
./mvnw test -pl fluss-server -Dtest=KvTabletTest -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "fix(server): logOffset strictly tracks CDC output count in retract branches"
```

---

### Task 4: Client — Per-Batch V2 Decision Based on MutationType

Remove the accumulator-level `v2Format` field. Decide V2 per-batch: first RETRACT record triggers V2 batch. V0 batch rejects RETRACT.

**Files:**
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java:114,120-158,611-633`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/write/KvWriteBatch.java:56-77,85-111`
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/write/WriterClient.java:109-111`
- Test: `fluss-client/src/test/java/org/apache/fluss/client/write/KvWriteBatchTest.java`

- [ ] **Step 1: Remove v2Format from RecordAccumulator**

In `RecordAccumulator.java`:

1. Delete the `private final boolean v2Format;` field (line 114)
2. Delete the 5-arg constructor (lines 128-158) — keep only the 4-arg constructor
3. In the 4-arg constructor, remove `this.v2Format = false;` (it was `this(conf, idempotenceManager, writerMetricGroup, clock, false)` — inline the body without v2Format)
4. In `createWriteBatch()` (line 632), replace `v2Format` with:

```java
writeRecord.getMutationType() == MutationType.RETRACT,
```

- [ ] **Step 2: KvWriteBatch rejects RETRACT in V0 batch**

In `KvWriteBatch.tryAppend()`, add at the beginning of the method (after `validateRecordConsistency`):

```java
// V0 batch cannot carry RETRACT records — caller must create a new V2 batch
if (!v2Format && writeRecord.getMutationType() == MutationType.RETRACT) {
    return false;
}
```

Note on the reject-and-retry flow: when `tryAppend()` returns `false`, `RecordAccumulator.append()` closes the current batch and calls `createWriteBatch()` with the rejected record. Since the record is RETRACT, `createWriteBatch()` passes `v2Format=true` to the new `KvWriteBatch`. The retry then succeeds on the new V2 batch.

- [ ] **Step 3: Update KvRecordBatchBuilder.builder() call sites**

In `KvWriteBatch` constructor, the `KvRecordBatchBuilder.builder()` call needs to pass `v2Format`:

```java
this.recordsBuilder =
        KvRecordBatchBuilder.builder(schemaId, writeLimit, outputView, kvFormat, v2Format);
```

- [ ] **Step 4: WriterClient — use 4-arg constructor**

`WriterClient.java:109-111` already uses the 4-arg constructor. After deleting the 5-arg constructor, this is the only constructor. No change needed here.

- [ ] **Step 5: Verify test compilation**

All existing test call sites (`SenderTest.java`, `RecordAccumulatorTest.java`) already use the 4-arg `RecordAccumulator` constructor — no changes needed. Verify compilation passes:

```bash
./mvnw test-compile -pl fluss-client -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

- [ ] **Step 6: Write test for V0 batch rejects RETRACT**

In `KvWriteBatchTest.java`, add test:

```java
@Test
void testV0BatchRejectsRetract() {
    // Create V0 batch (v2Format=false)
    KvWriteBatch batch = createKvWriteBatch(false);
    WriteRecord retractRecord = WriteRecord.forRetract(...);
    assertThat(batch.tryAppend(retractRecord, callback)).isFalse();
}

@Test
void testV2BatchAcceptsMixed() {
    // Create V2 batch (v2Format=true)
    KvWriteBatch batch = createKvWriteBatch(true);
    WriteRecord retractRecord = WriteRecord.forRetract(...);
    assertThat(batch.tryAppend(retractRecord, callback)).isTrue();
    WriteRecord upsertRecord = WriteRecord.forUpsert(...);
    assertThat(batch.tryAppend(upsertRecord, callback)).isTrue();
}
```

- [ ] **Step 7: Run client tests**

```bash
./mvnw test -pl fluss-client -Dtest=KvWriteBatchTest,RecordAccumulatorTest -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "fix(client): per-batch V2 decision based on MutationType, remove accumulator-level v2Format"
```

---

### Task 5: Client — Fix V2 Size Estimation for RETRACT Records

**Files:**
- Modify: `fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java:82,113`

- [ ] **Step 1: Fix forRetract() size estimation**

In `WriteRecord.forRetract()` (line 113), replace:

```java
int estimatedSizeInBytes = DefaultKvRecord.sizeOf(key, row) + RECORD_BATCH_HEADER_SIZE;
```

With:

```java
int estimatedSizeInBytes = DefaultKvRecord.sizeOfV2(key, row) + RECORD_BATCH_HEADER_SIZE;
```

`forUpsert()` and `forDelete()` keep using `sizeOf()` — they may end up in V2 batches (1 byte under-estimate) but this is the safe direction (batch closes earlier, never overflows).

- [ ] **Step 2: Run tests**

```bash
./mvnw test -pl fluss-client -Dtest=KvWriteBatchTest,RecordAccumulatorTest -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "fix(client): RETRACT WriteRecord uses sizeOfV2 for accurate size estimation"
```

---

### Task 6: Integration Verification

- [ ] **Step 1: Run all affected module tests**

```bash
./mvnw test -pl fluss-common,fluss-client,fluss-server -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository -DskipTests=false
```

- [ ] **Step 2: Spotless check**

```bash
./mvnw spotless:apply -Dmaven.repo.local=/Users/wangyang/Desktop/merge-engine/.cache/.m2/repository
```

- [ ] **Step 3: Squash or keep commits per preference**
