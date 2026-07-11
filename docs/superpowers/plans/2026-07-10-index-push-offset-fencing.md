# Offset-Fenced Index Push Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace row-level index versions and logical tombstones with a protocol-V1 target WriterState fence keyed by an opaque 128-bit WriterKey and `long batchSequence = windowEndOffset`, while preserving protocol V0 byte-for-byte for ordinary KV tables.

**Architecture:** Every KV table has an immutable `table.kv.idempotence-protocol-version`, defaulting to V0 unless explicitly set. V0 keeps the existing compact writer ID, int sequence, WriterState map, WAL, and snapshot; Index Tables explicitly select V1, whose opaque WriterKey is canonically derived from the source `TableBucket` and whose long sequence is the exclusive source WAL window end. Source committed snapshots bound local and remote WAL deletion, while target KV snapshots, V1 WriterState snapshots, and WAL are recovered as one provably covered state.

**Tech Stack:** Java, Maven, Fluss KV/WAL record formats, WriterState, RocksDB KV tablets, remote log tiering, ZooKeeper metadata, JUnit 5, AssertJ, JMH.

## Global Constraints

- Source progress and `IndexWindow.windowEndOffset` are exclusive next-read offsets.
- Protocol V1 `batchSequence` is `long` and exactly equals `windowEndOffset`; it is not a generated counter.
- `table.kv.idempotence-protocol-version` defaults to 0 for every KV table unless explicitly set; IndexTableDescriptorFactory explicitly writes 1 and runtime code never infers V1 from table type.
- Protocol V0 retains its current `writerId:int64`, `batchSequence:int32`, `Map<Long,...>`, exact-next validation, five-batch duplicate history, expiry, WAL, snapshot, and `Integer.MAX_VALUE -> 0` rollover.
- Protocol V1 uses an opaque 128-bit WriterKey, `batchSequence:int64`, latest-only sequence fence, explicit retirement, KV magic v1, target WAL magic v3, and WriterState snapshot v2.
- The index module alone canonically maps source `(partitionId?, bucketId)` to WriterKey; common PutKv, WAL, and WriterState code never interprets index fields.
- No source bucket count or writer field is added to Index Table metadata, table assignment, partition assignment, or leader-assignment RPCs.
- One `(WriterKey, windowEndOffset, targetBucket)` produces exactly one V1 KV batch and one target WAL batch.
- PutKv API v2 is a transport capability, not a protocol selector. API version, table protocol, and batch magic must agree; there is no V1-to-V0 fallback.
- Log Tables and ProduceLog remain on their existing idempotence path and reject an explicit KV idempotence protocol property.
- IndexSender uses `acks=-1` and `MergeMode.OVERWRITE`; index deletion is a null-value physical DELETE.
- `__source_offset`, `__index_deleted`, Index Table versioned merge settings, and `IndexEntryVisibilityFilter` are removed.
- Partitioned physical keys are `(indexColumns, basePrimaryKey, __partition_id)`; values retain the v3 partition tag.
- A stale V1 batch performs no row decode, KV mutation, or target WAL append, and waits for the dominating target WAL offset to enter HW.
- V1 WriterState retains one latest batch, never expires by V0 writer TTL, and is retired only by partition tombstone or Index Table deletion.
- Unknown tombstone state is not authoritative empty state. Partitioned internal writes remain not-ready until the empty or non-empty baseline is initialized.
- Only committed source KV snapshot progress may release local or remote raw WAL.
- An uncertain target WAL append fail-stops the replica; it never truncates prewrite and continues serving.
- Target recovery fails closed unless KV snapshot, WriterState snapshot, and continuous local/remote WAL jointly cover recovery.
- Do not change lake-table creation, deletion, or cleanup behavior. Index Tables remain non-lake internal tables.
- Preserve all existing user changes. Do not use broad reset, restore, checkout, or clean commands.
- Do not add fixed `Thread.sleep` synchronization. Use latches, fault injectors, manual clocks, or condition waiting.
- Each checkpoint commit has one concise message and no coauthor trailer.

---

## File And Interface Map

- `KvIdempotenceProtocol` resolves the immutable table property to `V0_COMPACT` or `V1_FENCED`.
- `WriterKey` is a generic opaque two-long value; `IndexWriterKey` alone owns canonical source-bucket encoding and decoding.
- `ConfigOptions` and `TableInfo` expose `table.kv.idempotence-protocol-version`, with default 0.
- `KvRecordBatch` magic v1 carries WriterKey plus a long sequence; V0 magic and parser stay byte-compatible.
- `LogRecordBatch` magic v3 carries WriterKey plus a long sequence; ordinary magic v0-v2 stays unchanged.
- `WriterStateManager` remains the lifecycle owner but preserves separate V0 compact and V1 fenced state representations.
- `KvTablet` owns the pre-decode stale path and prewrite/WAL append failure boundary.
- `IndexSpec` owns physical index row/key encoding; `IndexReplicator` owns source mutation grouping and windows.
- `TombstonedPartitionDiscriminator` owns writer/key/value partition validation and the tombstone gate.
- `IndexSourceReader` owns continuous local/remote raw-WAL reading without blocking the shared reader pool.
- `CompletedSnapshot.getMinRetainLogOffset()` is the common local/remote WAL deletion bound.

## Requirement Coverage

| Design requirement | Owning task |
|---|---|
| Default-V0 table protocol, V1 metadata, opaque WriterKey, canonical index codec | Task 1 |
| V1 KV and target WAL formats with byte-compatible V0 | Tasks 2-3 |
| Protocol-specific V0 compact and V1 fenced WriterState | Task 4 |
| Table/API/magic matrix, stale no-op, delayed HW acknowledgement | Task 5 |
| Uncertain target append fail-stop | Task 6 |
| Physical DELETE, pid-qualified key, dead row-version removal | Task 7 |
| Complete UPDATE groups, unaligned/output-aware windows, one batch per sequence | Task 8 |
| Tombstone initialization, pid validation, serialized writer retirement | Task 9 |
| Committed source retention floor and continuous raw remote replay | Task 10 |
| Joint target snapshot/WAL recovery and tiering coverage | Task 11 |
| Old/new leader races, model checking, partition lifecycle, metrics, capacity | Task 12 |

---

### Task 1: Table Protocol And Opaque WriterKey

**Files:**
- Create: `fluss-common/src/main/java/org/apache/fluss/metadata/KvIdempotenceProtocol.java`
- Create: `fluss-common/src/main/java/org/apache/fluss/record/WriterKey.java`
- Create: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexWriterKey.java`
- Create: `fluss-common/src/test/java/org/apache/fluss/metadata/KvIdempotenceProtocolTest.java`
- Create: `fluss-common/src/test/java/org/apache/fluss/record/WriterKeyTest.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexWriterKeyTest.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/metadata/TableInfo.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/utils/TableDescriptorValidation.java`
- Modify: `fluss-common/src/test/java/org/apache/fluss/metadata/TableInfoIndexTableTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/utils/TableDescriptorValidationTest.java`
- Modify later in Task 7: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexTableDescriptorFactory.java`

**Interfaces:**
- Produces: `ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION`, default `0`
- Produces: `KvIdempotenceProtocol.forVersion(int)` with `V0_COMPACT` and `V1_FENCED`
- Produces: `KvIdempotenceProtocol TableInfo.getKvIdempotenceProtocol()`
- Produces: immutable `WriterKey(long high, long low)` with exact equality and hash code
- Produces: `WriterKey IndexWriterKey.encode(TableBucket sourceBucket)`
- Produces: `IndexWriterKey.SourceBucket IndexWriterKey.decode(WriterKey writerKey)`

- [ ] **Step 1: Write protocol-default and table-validation tests**

```java
@Test
void testMissingKvIdempotenceProtocolDefaultsToV0() {
    TableInfo tableInfo = createPrimaryKeyTableInfo(Collections.emptyMap());
    assertThat(tableInfo.getKvIdempotenceProtocol())
            .isEqualTo(KvIdempotenceProtocol.V0_COMPACT);
}

@Test
void testExplicitProtocolV1IsResolved() {
    TableInfo tableInfo =
            createPrimaryKeyTableInfo(
                    Collections.singletonMap(
                            ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION.key(), "1"));
    assertThat(tableInfo.getKvIdempotenceProtocol())
            .isEqualTo(KvIdempotenceProtocol.V1_FENCED);
}

@Test
void testProtocolPropertyRejectedForLogTable() {
    TableDescriptor logTable =
            logTableDescriptorWithProperty(
                    ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION.key(), "0");
    assertThatThrownBy(
                    () ->
                            TableDescriptorValidation.validateTableDescriptor(
                                    logTable, 100, null))
            .isInstanceOf(InvalidConfigException.class)
            .hasMessageContaining("only supported for primary key tables");
}

@Test
void testProtocolV1RejectedForUserDataTable() {
    TableDescriptor dataTable = primaryKeyDataTableWithProtocol(1);
    assertThatThrownBy(
                    () ->
                            TableDescriptorValidation.validateTableDescriptor(
                                    dataTable, 100, null))
            .isInstanceOf(InvalidConfigException.class)
            .hasMessageContaining("protocol version 1 is reserved for system-managed Index Tables");
}
```

Add `KvIdempotenceProtocolTest` assertions that versions 0 and 1 resolve exactly and
versions `-1` and `2` throw `IllegalArgumentException`. Use existing test helpers in
`TableInfoIndexTableTest` and `TableDescriptorValidationTest`; do not create a second
descriptor builder framework.

- [ ] **Step 2: Write WriterKey and canonical index-codec tests**

```java
@Test
void testWriterKeyUsesAllBitsForEquality() {
    assertThat(new WriterKey(7L, 9L)).isEqualTo(new WriterKey(7L, 9L));
    assertThat(new WriterKey(7L, 9L)).isNotEqualTo(new WriterKey(8L, 9L));
    assertThat(new WriterKey(7L, 9L)).isNotEqualTo(new WriterKey(7L, 10L));
}

@Test
void testPartitionedSourceBucketRoundTrip() {
    WriterKey key = IndexWriterKey.encode(new TableBucket(99L, Long.MAX_VALUE, 3));
    assertThat(key.high()).isEqualTo(Long.MAX_VALUE);
    assertThat(key.low()).isEqualTo(Long.MIN_VALUE | 3L);
    IndexWriterKey.SourceBucket decoded = IndexWriterKey.decode(key);
    assertThat(decoded.getPartitionId()).hasValue(Long.MAX_VALUE);
    assertThat(decoded.getBucketId()).isEqualTo(3);
}

@Test
void testUnpartitionedSourceBucketRoundTrip() {
    WriterKey key = IndexWriterKey.encode(new TableBucket(99L, Integer.MAX_VALUE));
    assertThat(key).isEqualTo(new WriterKey(0L, Integer.MAX_VALUE));
    assertThat(IndexWriterKey.decode(key).getPartitionId()).isEmpty();
}

@Test
void testRejectsNonCanonicalWriterKeys() {
    assertThatThrownBy(() -> IndexWriterKey.decode(new WriterKey(1L, 3L)))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IndexWriterKey.decode(new WriterKey(1L, Long.MIN_VALUE | (1L << 40))))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> IndexWriterKey.encode(new TableBucket(99L, -1)))
            .isInstanceOf(IllegalArgumentException.class);
}
```

- [ ] **Step 3: Run the focused tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=KvIdempotenceProtocolTest,WriterKeyTest,IndexWriterKeyTest,TableInfoIndexTableTest,TableDescriptorValidationTest test
```

Expected: compilation fails because the protocol option, protocol class, WriterKey, and
IndexWriterKey do not exist.

- [ ] **Step 4: Implement the versioned protocol contract and default-zero metadata**

```java
public enum KvIdempotenceProtocol {
    V0_COMPACT(0),
    V1_FENCED(1);

    private final int version;

    KvIdempotenceProtocol(int version) {
        this.version = version;
    }

    public int version() {
        return version;
    }

    public static KvIdempotenceProtocol forVersion(int version) {
        switch (version) {
            case 0:
                return V0_COMPACT;
            case 1:
                return V1_FENCED;
            default:
                throw new IllegalArgumentException(
                        "Unsupported KV idempotence protocol version " + version);
        }
    }
}
```

Add:

```java
public static final ConfigOption<Integer> TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION =
        key("table.kv.idempotence-protocol-version")
                .intType()
                .defaultValue(0)
                .withDescription(
                        "The immutable KV idempotence protocol version. Version 0 is the "
                                + "compact writer-id protocol and is the default. Version 1 "
                                + "is the fenced WriterKey protocol reserved for system-managed "
                                + "Index Tables.");
```

`TableInfo` reads the defaulted value, so absence deterministically returns 0:

```java
public KvIdempotenceProtocol getKvIdempotenceProtocol() {
    return KvIdempotenceProtocol.forVersion(
            properties.get(ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION));
}
```

Add this validation while `hasPrimaryKey` and the original descriptor property map are
both available:

```java
boolean protocolExplicitlySet =
        tableDescriptor
                .getProperties()
                .containsKey(ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION.key());
if (!hasPrimaryKey && protocolExplicitlySet) {
    throw new InvalidConfigException(
            "table.kv.idempotence-protocol-version is only supported for primary key tables");
}
KvIdempotenceProtocol protocol;
try {
    protocol =
            KvIdempotenceProtocol.forVersion(
                    tableConf.get(ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION));
} catch (IllegalArgumentException e) {
    throw new InvalidConfigException(e.getMessage());
}
if (protocol == KvIdempotenceProtocol.V1_FENCED && !tableDescriptor.isIndexTable()) {
    throw new InvalidConfigException(
            "KV idempotence protocol version 1 is reserved for system-managed Index Tables");
}
```

- [ ] **Step 5: Implement opaque WriterKey and index-owned canonical encoding**

```java
public final class WriterKey {
    private final long high;
    private final long low;

    public WriterKey(long high, long low) {
        this.high = high;
        this.low = low;
    }

    public long high() {
        return high;
    }

    public long low() {
        return low;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WriterKey)) {
            return false;
        }
        WriterKey that = (WriterKey) other;
        return high == that.high && low == that.low;
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(high) + Long.hashCode(low);
    }
}
```

Implement the index-owned codec without exposing its layout to common WriterState code:

```java
public final class IndexWriterKey {
    private static final long PARTITIONED_MASK = Long.MIN_VALUE;
    private static final long BUCKET_MASK = Integer.MAX_VALUE;
    private static final long RESERVED_MASK = ~(PARTITIONED_MASK | BUCKET_MASK);

    private IndexWriterKey() {}

    public static WriterKey encode(TableBucket sourceBucket) {
        int bucketId = sourceBucket.getBucket();
        checkArgument(bucketId >= 0, "bucketId must be non-negative");
        Long partitionId = sourceBucket.getPartitionId();
        if (partitionId == null) {
            return new WriterKey(0L, bucketId);
        }
        checkArgument(partitionId >= 0L, "partitionId must be non-negative");
        return new WriterKey(partitionId, PARTITIONED_MASK | (long) bucketId);
    }

    public static SourceBucket decode(WriterKey writerKey) {
        long high = writerKey.high();
        long low = writerKey.low();
        checkArgument((low & RESERVED_MASK) == 0L, "WriterKey has reserved bits set");
        int bucketId = (int) (low & BUCKET_MASK);
        if ((low & PARTITIONED_MASK) == 0L) {
            checkArgument(high == 0L, "Unpartitioned WriterKey must have high=0");
            return new SourceBucket(null, bucketId);
        }
        checkArgument(high >= 0L, "partitionId must be non-negative");
        return new SourceBucket(high, bucketId);
    }

    public static final class SourceBucket {
        private final @Nullable Long partitionId;
        private final int bucketId;

        private SourceBucket(@Nullable Long partitionId, int bucketId) {
            this.partitionId = partitionId;
            this.bucketId = bucketId;
        }

        public OptionalLong getPartitionId() {
            return partitionId == null
                    ? OptionalLong.empty()
                    : OptionalLong.of(partitionId);
        }

        public int getBucketId() {
            return bucketId;
        }
    }
}
```

The source table ID is deliberately not encoded.

- [ ] **Step 6: Run focused and compatibility tests**

Run the Step 3 command. Expected: PASS. Then run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=TableDescriptorValidationTest,TableDescriptorTest,TableInfoIndexTableTest test
```

Expected: PASS with existing primary-key and Log Table defaults unchanged.

- [ ] **Step 7: Commit the protocol boundary**

```bash
git add fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java \
  fluss-common/src/main/java/org/apache/fluss/metadata/KvIdempotenceProtocol.java \
  fluss-common/src/main/java/org/apache/fluss/metadata/TableInfo.java \
  fluss-common/src/main/java/org/apache/fluss/record/WriterKey.java \
  fluss-common/src/test/java/org/apache/fluss/metadata/KvIdempotenceProtocolTest.java \
  fluss-common/src/test/java/org/apache/fluss/metadata/TableInfoIndexTableTest.java \
  fluss-common/src/test/java/org/apache/fluss/record/WriterKeyTest.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexWriterKey.java \
  fluss-server/src/main/java/org/apache/fluss/server/utils/TableDescriptorValidation.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexWriterKeyTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/utils/TableDescriptorValidationTest.java
git commit -m "Add KV idempotence protocols"
```

---

### Task 2: Protocol V1 KV Batch Without V0 Layout Changes

**Files:**
- Create: `fluss-common/src/main/java/org/apache/fluss/record/FencedKvRecordBatch.java`
- Create: `fluss-common/src/main/java/org/apache/fluss/record/FencedKvRecordBatchBuilder.java`
- Create: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatchReader.java`
- Create: `fluss-common/src/test/java/org/apache/fluss/record/FencedKvRecordBatchTest.java`
- Create: `fluss-common/src/test/java/org/apache/fluss/record/KvRecordBatchBuilderTest.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatch.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecordBatch.java`
- Modify: `fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordBatchTest.java`

**Interfaces:**
- Produces: `KvRecordBatch.KV_MAGIC_VALUE_V1`
- Preserves: `long KvRecordBatch.writerId()` and `int KvRecordBatch.batchSequence()` for V0
- Produces: `int KvRecordBatch.idempotenceProtocolVersion()`
- Produces: `WriterKey KvRecordBatch.fencedWriterKey()` for V1 only
- Produces: `long KvRecordBatch.fencedSequence()` for V1 only
- Produces: `KvRecordBatch KvRecordBatchReader.pointToByteBuffer(ByteBuffer)`
- Produces: `FencedKvRecordBatchBuilder.builder(int, int, AbstractPagedOutputView, KvFormat)`
- Produces: `void FencedKvRecordBatchBuilder.setWriterState(WriterKey, long)`

- [ ] **Step 1: Add byte-exact V0 compatibility tests**

Extend `DefaultKvRecordBatchTest` and `KvRecordBatchBuilderTest`:

```java
@Test
void testV0HeaderAndAccessorsRemainByteCompatible() throws Exception {
    byte[] bytes = buildV0BatchBytes(33L, Integer.MAX_VALUE);
    KvRecordBatch batch = KvRecordBatchReader.pointToByteBuffer(ByteBuffer.wrap(bytes));
    assertThat(batch.getClass()).isEqualTo(DefaultKvRecordBatch.class);
    assertThat(batch.magic()).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V0);
    assertThat(batch.idempotenceProtocolVersion()).isZero();
    assertThat(batch.writerId()).isEqualTo(33L);
    assertThat(batch.batchSequence()).isEqualTo(Integer.MAX_VALUE);
    assertThat(DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE).isEqualTo(28);
    assertThat(bytes).isEqualTo(EXPECTED_V0_FIXTURE);
}

@Test
void testCurrentBuilderStillProducesV0() throws Exception {
    KvRecordBatch batch = buildWithExistingKvRecordBatchBuilder();
    assertThat(batch.magic()).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V0);
    assertThat(batch.idempotenceProtocolVersion()).isZero();
}
```

Define `EXPECTED_V0_FIXTURE` from the current builder output before implementation and
commit the literal fixture in the test. Do not regenerate it from the code under test.

- [ ] **Step 2: Add V1 round-trip, bounds, and parser-rejection tests**

```java
@ParameterizedTest
@ValueSource(longs = {0L, 1L, Integer.MAX_VALUE, 2147483648L, Long.MAX_VALUE})
void testV1WriterKeyAndSequenceRoundTrip(long sequence) throws Exception {
    WriterKey writerKey = new WriterKey(33L, Long.MIN_VALUE | 7L);
    KvRecordBatch batch = buildV1Batch(writerKey, sequence);
    assertThat(batch).isInstanceOf(FencedKvRecordBatch.class);
    assertThat(batch.magic()).isEqualTo(KvRecordBatch.KV_MAGIC_VALUE_V1);
    assertThat(batch.idempotenceProtocolVersion()).isEqualTo(1);
    assertThat(batch.fencedWriterKey()).isEqualTo(writerKey);
    assertThat(batch.fencedSequence()).isEqualTo(sequence);
    assertThat(FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE).isEqualTo(40);
    batch.ensureValid();
}

@Test
void testV1RejectsNegativeSequence() {
    assertThatThrownBy(
                    () ->
                            FencedKvRecordBatchBuilder.builder(
                                            1, 1024, outputView, KvFormat.COMPACTED)
                                    .setWriterState(new WriterKey(1L, 2L), -1L))
            .isInstanceOf(IllegalArgumentException.class);
}

@Test
void testReaderRejectsUnknownMagicBeforeHeaderAccess() {
    byte[] bytes = minimumBatchWithMagic((byte) 2);
    assertThatThrownBy(
                    () -> KvRecordBatchReader.pointToByteBuffer(ByteBuffer.wrap(bytes)))
            .isInstanceOf(CorruptMessageException.class)
            .hasMessageContaining("Unsupported KV batch magic 2");
}
```

- [ ] **Step 3: Run format tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -am \
  -Dspotless.check.skip=true \
  -Dtest=DefaultKvRecordBatchTest,KvRecordBatchBuilderTest,FencedKvRecordBatchTest test
```

Expected: compilation fails because V1 classes, reader, and V1 accessors do not exist.

- [ ] **Step 4: Implement separate V0 and V1 parsers and builders**

Keep `DefaultKvRecordBatch` offsets and `KvRecordBatchBuilder` unchanged. Add default V0
methods to `KvRecordBatch`:

```java
default int idempotenceProtocolVersion() {
    return 0;
}

default WriterKey fencedWriterKey() {
    throw new UnsupportedOperationException("V0 batch has no fenced WriterKey");
}

default long fencedSequence() {
    throw new UnsupportedOperationException("V0 batch has no fenced sequence");
}
```

`FencedKvRecordBatch` uses fixed offsets:

```text
length=0, magic=4, crc=5, schemaId=9, attributes=11,
writerKeyHigh=12, writerKeyLow=20, sequence=28,
recordCount=36, records=40
```

Its CRC starts at schema ID exactly like V0. `FencedKvRecordBatchBuilder` must share
`DefaultKvRecord` encoding but own its 40-byte header and require writer state before
build. `KvRecordBatchReader` peeks length and magic, validates the minimum header length,
then returns `DefaultKvRecordBatch` for magic 0 or `FencedKvRecordBatch` for magic 1. It
must reject every other magic without invoking a version-specific accessor.

- [ ] **Step 5: Run format and downstream compatibility tests**

Run the Step 3 command. Expected: PASS. Then run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client,fluss-server -am \
  -Dspotless.check.skip=true -DskipTests compile
```

Expected: BUILD SUCCESS with no changes required in `KvWriteBatch`, RecordAccumulator,
or the ordinary client writer.

- [ ] **Step 6: Commit the V1 KV format**

```bash
git add fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatch.java \
  fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecordBatch.java \
  fluss-common/src/main/java/org/apache/fluss/record/FencedKvRecordBatch.java \
  fluss-common/src/main/java/org/apache/fluss/record/FencedKvRecordBatchBuilder.java \
  fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatchReader.java \
  fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordBatchTest.java \
  fluss-common/src/test/java/org/apache/fluss/record/FencedKvRecordBatchTest.java \
  fluss-common/src/test/java/org/apache/fluss/record/KvRecordBatchBuilderTest.java
git commit -m "Add fenced KV batch format"
```

---

### Task 3: Protocol V1 Target WAL Format Without ProduceLog Changes

**Files:**
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/LogRecordBatch.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/LogRecordBatchFormat.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/DefaultLogRecordBatch.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/FileLogInputStream.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/FileLogProjection.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsRowBuilder.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsArrowBuilder.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsCompactedBuilder.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsIndexedBuilder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/wal/WalBuilder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/wal/ArrowWalBuilder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/wal/CompactedWalBuilder.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/wal/IndexWalBuilder.java`
- Modify: `fluss-common/src/test/java/org/apache/fluss/record/DefaultLogRecordBatchTest.java`
- Modify: `fluss-common/src/test/java/org/apache/fluss/record/LogRecordBatchFormatTest.java`
- Modify: `fluss-common/src/test/java/org/apache/fluss/record/FileLogInputStreamTest.java`
- Modify: `fluss-common/src/test/java/org/apache/fluss/record/FileLogProjectionTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/kv/wal/ArrowWalBuilderTest.java`

**Interfaces:**
- Produces: `LogRecordBatchFormat.LOG_MAGIC_VALUE_V3`
- Preserves: `long LogRecordBatch.writerId()` and `int LogRecordBatch.batchSequence()` for v0-v2
- Produces: `int LogRecordBatch.idempotenceProtocolVersion()`
- Produces: `WriterKey LogRecordBatch.fencedWriterKey()` for v3 only
- Produces: `long LogRecordBatch.fencedSequence()` for v3 only
- Produces: `void WalBuilder.setFencedWriterState(WriterKey writerKey, long sequence)`

- [ ] **Step 1: Add byte-exact v0-v2 and ProduceLog compatibility tests**

Retain every current header-offset assertion and add literal fixtures for representative
v0 and v2 batches:

```java
@ParameterizedTest
@ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
void testExistingWriterAccessorsRemainCompact(byte magic) throws Exception {
    LogRecordBatch batch = buildOrdinaryBatch(magic, 7L, Integer.MAX_VALUE);
    assertThat(batch.writerId()).isEqualTo(7L);
    assertThat(batch.batchSequence()).isEqualTo(Integer.MAX_VALUE);
    assertThat(batch.idempotenceProtocolVersion()).isZero();
}

@Test
void testProduceLogBuilderCannotCreateV3() throws Exception {
    MemoryLogRecords records = buildThroughExistingClientWriter();
    assertThat(records.batches())
            .allSatisfy(batch -> assertThat(batch.magic()).isLessThan(LOG_MAGIC_VALUE_V3));
}
```

- [ ] **Step 2: Add v3 WriterKey round-trip tests across every reader**

```java
@Test
void testV3WriterKeyAndLongSequenceSurviveFileRoundTrip() throws Exception {
    WriterKey key = new WriterKey(17L, Long.MIN_VALUE | 3L);
    long sequence = (long) Integer.MAX_VALUE + 17L;
    MemoryLogRecords records = buildFencedRecords(key, sequence);
    LogRecordBatch batch = writeAndRead(records).batches().iterator().next();
    assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V3);
    assertThat(batch.idempotenceProtocolVersion()).isEqualTo(1);
    assertThat(batch.fencedWriterKey()).isEqualTo(key);
    assertThat(batch.fencedSequence()).isEqualTo(sequence);
}
```

Repeat the same key and sequence assertions through `FileLogInputStream` and
`FileLogProjection`. Add a corruption test for magic 3 whose declared size is smaller
than the v3 fixed header.

- [ ] **Step 3: Run format tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=DefaultLogRecordBatchTest,LogRecordBatchFormatTest,FileLogInputStreamTest,FileLogProjectionTest,ArrowWalBuilderTest test
```

Expected: compilation fails on v3, fenced accessors, and fenced WalBuilder state.

- [ ] **Step 4: Implement v3 as a protocol-specific extension of v2**

V3 keeps every v2 field through `lastOffsetDelta`, then stores:

```text
writerKeyHigh:int64
writerKeyLow:int64
sequence:int64
recordCount:int32
statisticsLength:int32
```

V3 therefore adds 12 bytes relative to v2. Add explicit v3 cases to every offset and
header-size switch. Keep existing v0-v2 constants and methods unchanged. Add default
throwing fenced accessors to `LogRecordBatch`; `DefaultLogRecordBatch` overrides them only
when `magic == LOG_MAGIC_VALUE_V3`.

All in-memory row builders retain their existing `setWriterState(long, int)` method and
add:

```java
public void setFencedWriterState(WriterKey writerKey, long sequence) {
    checkState(magic == LOG_MAGIC_VALUE_V3, "Fenced writer state requires WAL magic v3");
    checkArgument(sequence >= 0L, "fenced sequence must be non-negative");
    this.writerKey = checkNotNull(writerKey);
    this.fencedSequence = sequence;
}
```

Add explicit v3 factories/constructor overloads to the in-memory builders while retaining
every existing factory default. Expose the corresponding method from every server
`WalBuilder`; Task 5 selects the v3 factory when the owning table protocol is V1.
Existing client writer classes must not call it and must not be modified merely to
accommodate v3.

- [ ] **Step 5: Run format, client, and server compatibility tests**

Run the Step 3 command. Expected: PASS. Then run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client,fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=RecordAccumulatorTest,SenderTest,LogTabletTest test
```

Expected: PASS with existing ProduceLog batches and V0 WriterState behavior unchanged.

- [ ] **Step 6: Commit the V1 target WAL format**

```bash
git add fluss-common/src/main/java/org/apache/fluss/record/LogRecordBatch.java \
  fluss-common/src/main/java/org/apache/fluss/record/LogRecordBatchFormat.java \
  fluss-common/src/main/java/org/apache/fluss/record/DefaultLogRecordBatch.java \
  fluss-common/src/main/java/org/apache/fluss/record/FileLogInputStream.java \
  fluss-common/src/main/java/org/apache/fluss/record/FileLogProjection.java \
  fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsRowBuilder.java \
  fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsArrowBuilder.java \
  fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsCompactedBuilder.java \
  fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsIndexedBuilder.java \
  fluss-common/src/test/java/org/apache/fluss/record/DefaultLogRecordBatchTest.java \
  fluss-common/src/test/java/org/apache/fluss/record/LogRecordBatchFormatTest.java \
  fluss-common/src/test/java/org/apache/fluss/record/FileLogInputStreamTest.java \
  fluss-common/src/test/java/org/apache/fluss/record/FileLogProjectionTest.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/wal \
  fluss-server/src/test/java/org/apache/fluss/server/kv/wal/ArrowWalBuilderTest.java
git commit -m "Add fenced target WAL format"
```

---

### Task 4: Protocol-Specific WriterState And Snapshot V2

**Files:**
- Create: `fluss-server/src/main/java/org/apache/fluss/server/log/FencedWriterStateEntry.java`
- Create: `fluss-server/src/main/java/org/apache/fluss/server/log/FencedWriterAppendInfo.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/WriterStateManager.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/log/WriterStateManagerTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/log/WriterSnapshotMapJsonSerdeTest.java`

**Interfaces:**
- Preserves: existing three-argument `WriterStateManager` constructor as V0
- Produces: `WriterStateManager(TableBucket, File, int, KvIdempotenceProtocol)`
- Preserves: existing V0 `lastEntry(long)`, `prepareUpdate(long)`, and `update(WriterAppendInfo)`
- Produces: `Optional<FencedWriterStateEntry> lastFencedEntry(WriterKey)`
- Produces: `Optional<FencedWriterStateEntry> findStaleFencedBatch(WriterKey, long)`
- Produces: `FencedWriterAppendInfo prepareFencedUpdate(WriterKey)`
- Produces: `void FencedWriterAppendInfo.append(long sequence, long targetWalOffset, long timestamp)`
- Produces: `void updateFenced(FencedWriterAppendInfo)`
- Produces: `void removeFencedWriters(Predicate<WriterKey>)`

- [ ] **Step 1: Add V0 no-regression tests around data structures and snapshots**

```java
@Test
void testThreeArgumentConstructorRemainsV0Compact() throws Exception {
    WriterStateManager manager = new WriterStateManager(tableBucket, logDir, 1000);
    appendV0(manager, 5L, 0, 8L);
    assertThat(manager.activeWriters()).containsOnlyKeys(5L);
    assertThat(manager.protocol()).isEqualTo(KvIdempotenceProtocol.V0_COMPACT);
}

@Test
void testV0SnapshotFixtureRoundTripsByteForByte() throws Exception {
    WriterStateManager manager = new WriterStateManager(tableBucket, logDir, 1000);
    WriterAppendInfo appendInfo = manager.prepareUpdate(5L);
    appendInfo.appendDataBatch(
            7, new LogOffsetMetadata(8L), 8L, false, false, 9L);
    manager.update(appendInfo);
    manager.updateMapEndOffset(9L);
    manager.takeSnapshot();
    assertThat(Files.readAllBytes(writerSnapshotFile(logDir, 9L).toPath()))
            .isEqualTo(V0_SNAPSHOT_FIXTURE);
}
```

Use the repository's current writer snapshot bytes as `V0_SNAPSHOT_FIXTURE`; do not
generate the expected bytes with the modified serde.

- [ ] **Step 2: Add V1 fence, expiry, and snapshot tests**

```java
@Test
void testV1SparseSequenceUsesLatestFenceOnly() throws Exception {
    WriterStateManager manager = fencedManager();
    WriterKey key = new WriterKey(4L, 5L);
    appendV1(manager, key, 100L, 10L);
    appendV1(manager, key, 500L, 20L);
    appendV1(manager, key, (long) Integer.MAX_VALUE + 1L, 30L);
    FencedWriterStateEntry entry = manager.lastFencedEntry(key).orElseThrow(AssertionError::new);
    assertThat(entry.lastSequence()).isEqualTo((long) Integer.MAX_VALUE + 1L);
    assertThat(entry.dominatingTargetWalOffset()).isEqualTo(30L);
    assertThat(manager.findStaleFencedBatch(key, 500L)).contains(entry);
    assertThat(manager.findStaleFencedBatch(key, entry.lastSequence())).contains(entry);
    assertThat(manager.findStaleFencedBatch(key, entry.lastSequence() + 1L)).isEmpty();
}

@Test
void testV1WriterDoesNotExpireAndCanBeExplicitlyRetired() throws Exception {
    WriterStateManager manager = fencedManager();
    WriterKey key = new WriterKey(4L, 5L);
    appendV1(manager, key, 100L, 10L);
    manager.removeExpiredWriters(Long.MAX_VALUE);
    assertThat(manager.lastFencedEntry(key)).isPresent();
    manager.removeFencedWriters(key::equals);
    assertThat(manager.lastFencedEntry(key)).isEmpty();
}

@Test
void testV1SnapshotRoundTripPreservesFullWriterKeyAndLongSequence() throws Exception {
    WriterStateManager manager = fencedManager();
    WriterKey key = new WriterKey(Long.MAX_VALUE, Long.MIN_VALUE | 3L);
    appendV1(manager, key, 2147483648L, 30L);
    manager.takeSnapshot();
    WriterStateManager recovered = fencedManager();
    recovered.truncateAndReload(0L, 31L, Long.MAX_VALUE);
    FencedWriterStateEntry entry = recovered.lastFencedEntry(key).orElseThrow(AssertionError::new);
    assertThat(entry.lastSequence()).isEqualTo(2147483648L);
    assertThat(entry.dominatingTargetWalOffset()).isEqualTo(30L);
}
```

- [ ] **Step 3: Run WriterState tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=WriterStateManagerTest,WriterSnapshotMapJsonSerdeTest test
```

Expected: compilation fails because the fenced entry, protocol constructor, and V1 APIs
do not exist.

- [ ] **Step 4: Implement a V1 entry without widening the V0 entry**

```java
public final class FencedWriterStateEntry {
    private final WriterKey writerKey;
    private final long lastSequence;
    private final long dominatingTargetWalOffset;
    private final long lastTimestamp;

    public FencedWriterStateEntry(
            WriterKey writerKey,
            long lastSequence,
            long dominatingTargetWalOffset,
            long lastTimestamp) {
        checkArgument(lastSequence >= 0L, "lastSequence must be non-negative");
        this.writerKey = checkNotNull(writerKey);
        this.lastSequence = lastSequence;
        this.dominatingTargetWalOffset = dominatingTargetWalOffset;
        this.lastTimestamp = lastTimestamp;
    }

    public WriterKey writerKey() {
        return writerKey;
    }

    public long lastSequence() {
        return lastSequence;
    }

    public long dominatingTargetWalOffset() {
        return dominatingTargetWalOffset;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FencedWriterStateEntry)) {
            return false;
        }
        FencedWriterStateEntry that = (FencedWriterStateEntry) other;
        return lastSequence == that.lastSequence
                && dominatingTargetWalOffset == that.dominatingTargetWalOffset
                && lastTimestamp == that.lastTimestamp
                && writerKey.equals(that.writerKey);
    }

    @Override
    public int hashCode() {
        int result = writerKey.hashCode();
        result = 31 * result + Long.hashCode(lastSequence);
        result = 31 * result + Long.hashCode(dominatingTargetWalOffset);
        return 31 * result + Long.hashCode(lastTimestamp);
    }
}
```

Keep `WriterStateEntry`, `WriterAppendInfo`, and `Map<Long, WriterStateEntry>` unchanged.
`WriterStateManager` stores exactly one active map based on its constructor protocol.
Every V0 method checks V0 and every fenced method checks V1, failing with
`IllegalStateException` on cross-protocol use. The existing constructor delegates to
`V0_COMPACT`. `removeExpiredWriters` executes existing code for V0 and returns without
mutation for V1. `writerIdCount()` reports the size of the selected protocol map so
existing metrics continue to count V1 WriterKeys without maintaining two live stores.

`FencedWriterAppendInfo` captures the current entry at prepare time. Its `append` method
requires `sequence >= 0`, and when a current entry exists requires
`sequence > current.lastSequence()`. It creates exactly one updated
`FencedWriterStateEntry` using the supplied inclusive target WAL offset. `updateFenced`
replaces the map value only after target WAL append succeeds; it rejects an append info
whose updated entry is absent.

- [ ] **Step 5: Implement snapshot v2 and strict protocol matching**

Continue writing the exact current snapshot for V0. Write V1 snapshots as:

```json
{"version":2,"kv_idempotence_protocol_version":1,"writer_entries":[{"writer_key_high":4,"writer_key_low":5,"last_sequence":2147483648,"last_target_wal_offset":30,"last_timestamp":1}]}
```

V0 rejects snapshot version 2. V1 rejects snapshot version 1, a missing protocol field,
duplicate WriterKeys, a negative sequence, or a malformed key field. Loading must never
convert a V0 writer ID into a V1 WriterKey. Corrupt-snapshot fallback rules remain for
Task 11; this task returns the parse error to the caller.

- [ ] **Step 6: Run V0, V1, and LogTablet regression tests**

Run the Step 3 command. Expected: PASS. Then run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=WriterStateManagerTest,WriterSnapshotMapJsonSerdeTest,LogTabletTest test
```

Expected: PASS, especially existing V0 duplicate, out-of-order, rollover, expiry, and
snapshot assertions.

- [ ] **Step 7: Commit protocol-specific WriterState**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/log/FencedWriterAppendInfo.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/FencedWriterStateEntry.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/WriterStateManager.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/WriterSnapshotMapJsonSerdeTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/WriterStateManagerTest.java
git commit -m "Add fenced WriterState protocol"
```

---

### Task 5: Target Batch Contract And Stale Fast Path

**Files:**
- Modify: `fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/ApiKeys.java`
- Modify: `fluss-rpc/src/test/java/org/apache/fluss/rpc/protocol/ApiKeysTest.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogLoader.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogManager.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogAppendInfo.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexSender.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/log/LogTabletTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSenderTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/tablet/TabletServiceITCase.java`

**Interfaces:**
- Consumes: `KvIdempotenceProtocol`, V1 KV magic, V3 WAL magic, and V1 WriterState
- Produces: `Optional<FencedWriterStateEntry> LogTablet.findStaleFencedBatch(WriterKey, long)`
- Produces: `LogAppendInfo LogAppendInfo.duplicatedAt(long targetWalOffset, long timestamp)`
- Produces: exact table/API/magic acceptance matrix from the spec
- Produces: PutKv API v2 capability gate keyed by concrete target server/gateway

- [ ] **Step 1: Add the complete API/table/magic compatibility matrix tests**

In `ReplicaTest` or `TabletServiceITCase`, parameterize these six cases:

```java
@ParameterizedTest
@MethodSource("putKvProtocolMatrix")
void testPutKvProtocolMatrix(
        KvIdempotenceProtocol tableProtocol,
        short apiVersion,
        byte batchMagic,
        boolean accepted) {
    ThrowingCallable call = () -> put(tableProtocol, apiVersion, batchMagic);
    if (accepted) {
        assertThatCode(call).doesNotThrowAnyException();
    } else {
        assertThatThrownBy(call).isInstanceOf(UnsupportedVersionException.class);
        assertThat(kvPrewriteCount()).isZero();
    }
}
```

The matrix is:

```text
V0 + API 1 + magic 0 -> accept
V0 + API 2 + magic 0 -> accept
V0 + API 2 + magic 1 -> reject
V1 + API 1 + magic 1 -> reject
V1 + API 2 + magic 0 -> reject
V1 + API 2 + magic 1 -> accept
```

Add a V1-under-API-1 payload whose bytes after magic are deliberately too short for a V1
header. Assert `UnsupportedVersionException`, not buffer underflow, proving rejection
occurs before V1 field access. Update `ApiKeysTest` to assert PUT_KV max version 2 while
its min remains 0.

- [ ] **Step 2: Add stale pre-decode and LogTablet revalidation tests**

```java
@Test
void testStaleV1BatchDoesNotDecodeOrAppend() throws Exception {
    WriterKey key = new WriterKey(9L, Long.MIN_VALUE | 3L);
    appendFenced(target, key, 500L, validPut("k", "new"));
    long leo = logTablet.localLogEndOffset();
    KvRecordBatch malformedButHeaderValid = malformedV1Batch(key, 100L);
    LogAppendInfo result =
            target.putAsLeader(malformedButHeaderValid, null, MergeMode.OVERWRITE);
    assertThat(result.duplicated()).isTrue();
    assertThat(result.lastOffset()).isEqualTo(
            logTablet.writerStateManager()
                    .lastFencedEntry(key)
                    .orElseThrow(AssertionError::new)
                    .dominatingTargetWalOffset());
    assertThat(logTablet.localLogEndOffset()).isEqualTo(leo);
    assertThat(lookup("k")).contains("new");
}

@Test
void testLogLockRevalidationTurnsFreshPrecheckIntoStale() throws Exception {
    WriterKey key = new WriterKey(9L, 3L);
    pauseAfterKvPrecheck();
    CompletableFuture<LogAppendInfo> older = putAsync(key, 100L, put("k", "old"));
    appendDirectlyUnderLogLock(key, 200L, put("k", "new"));
    resumeAfterKvPrecheck();
    assertThat(older.get()).matches(LogAppendInfo::duplicated);
    assertThat(lookup("k")).contains("new");
    assertThat(kvPrewriteTruncateReason()).isEqualTo(TruncateReason.DUPLICATED);
}
```

- [ ] **Step 3: Add IndexSender capability-cache and no-fallback tests**

Assert requests use `acks=-1`, `MergeMode.OVERWRITE`, and preserve one encoded V1 batch.
A gateway advertising PutKv max version 1 receives no PutKv payload. A gateway advertising
version 2 receives the exact V1 bytes. Then:

```java
@Test
void testNegativeCapabilityExpiresAndLeaderReplacementInvalidatesCache() {
    gateway(server1).advertisePutKvMaxVersion((short) 1);
    enqueueV1Batch(targetBucketOn(server1));
    runSender();
    assertThat(gateway(server1).putKvRequests()).isEmpty();

    gateway(server1).advertisePutKvMaxVersion((short) 2);
    manualClock.advanceMillis(RETRY_BACKOFF_MS);
    runSender();
    assertThat(gateway(server1).putKvRequests()).hasSize(1);

    moveLeaderTo(server2);
    gateway(server2).advertisePutKvMaxVersion((short) 1);
    enqueueV1Batch(targetBucketOn(server2));
    runSender();
    assertThat(gateway(server2).putKvRequests()).isEmpty();
}
```

- [ ] **Step 4: Run focused tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=ApiKeysTest,LogTabletTest,KvTabletTest,ReplicaTest,IndexSenderTest,TabletServiceITCase test
```

- [ ] **Step 5: Propagate the immutable table protocol without table-type inference**

For a primary-key table, production replica creation passes only the explicit/defaulted
table property:

```java
KvIdempotenceProtocol protocol = tableInfo.getKvIdempotenceProtocol();
```

Log Tables keep the existing `WriterStateManager` constructor and therefore V0 behavior;
they do not read the KV table property. Add protocol parameters through `Replica`,
`LogManager`, and `LogLoader` only for primary-key table construction. Existing test
helpers default to V0. Follower append and recovery reject WAL magic v3 unless the owning
KV table protocol is V1, and reject v0-v2 writer batches for V1.

- [ ] **Step 6: Implement API-aware batch parsing and the table contract**

Bump `ApiKeys.PUT_KV` max version from 1 to 2. Change TabletService to pass
`currentSession().getApiVersion()` into batch parsing. `ServerRpcMessageUtils` peeks the
batch magic first:

```java
if (magic == KvRecordBatch.KV_MAGIC_VALUE_V1 && apiVersion < 2) {
    throw new UnsupportedVersionException(
            "KV idempotence protocol V1 requires PutKv API v2");
}
KvRecordBatch records = KvRecordBatchReader.pointToByteBuffer(recordsBuffer);
```

`Replica.putRecordsToLeader` compares `tableInfo.getKvIdempotenceProtocol()` with
`records.idempotenceProtocolVersion()`. V0 accepts only magic 0. V1 accepts only magic 1,
`MergeMode.OVERWRITE`, and `requiredAcks == -1`. Use `UnsupportedVersionException` for
API/protocol incompatibility and `InvalidTableException` for merge/acks violations. All
checks occur before `KvTablet.putAsLeader`.

- [ ] **Step 7: Add the pre-decode stale path and append revalidation**

At the beginning of `KvTablet.putAsLeader`, under `kvLock` and after header/table
validation:

```java
if (kvRecords.idempotenceProtocolVersion() == 1) {
    Optional<FencedWriterStateEntry> stale =
            logTablet.findStaleFencedBatch(
                    kvRecords.fencedWriterKey(), kvRecords.fencedSequence());
    if (stale.isPresent()) {
        FencedWriterStateEntry entry = stale.get();
        return LogAppendInfo.duplicatedAt(
                entry.dominatingTargetWalOffset(), entry.lastTimestamp());
    }
}
```

When writing the target WAL, call
`walBuilder.setFencedWriterState(kvRecords.fencedWriterKey(),
kvRecords.fencedSequence())`. `LogTablet.append` validates v3 under its lock against
`WriterStateManager.findStaleFencedBatch`; if state advanced after precheck, it returns
`LogAppendInfo.duplicatedAt(...)`, and KvTablet truncates only that known-stale prewrite.
On fresh append, update V1 WriterState only after local WAL append succeeds.

`KvTablet.createWalBuilder` selects the explicit magic-v3 builder factory only when
`kvRecords.idempotenceProtocolVersion() == 1`; its existing V0 branch and all ProduceLog
builders retain their current magic selection.

- [ ] **Step 8: Gate IndexSender on the concrete target's PutKv v2 capability**

`IndexSender` caches capability by concrete target server ID plus gateway identity. It
calls:

```java
gateway.apiVersions(
        new ApiVersionsRequest()
                .setClientSoftwareName("fluss-index-replicator")
                .setClientSoftwareVersion("2"))
```

Find the PUT_KV entry and require `maxVersion >= 2` before sending a V1 batch. An
incompatible or unresolved target is retried with normal backoff and an error metric; V1
bytes are never sent to it. Cache a positive result only for that gateway instance. A
negative/error result expires at the retry deadline and is queried again. Any
leader/gateway replacement removes the old entry. Never rebuild or resend the batch as
V0.

- [ ] **Step 9: Run focused and V0 regression tests**

Run the Step 4 command. Expected: PASS. Then run:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=RecordAccumulatorTest,SenderTest,WriterStateManagerTest,LogTabletTest,KvTabletTest test
```

Expected: PASS with V0 clients, Log Tables, and ProduceLog unchanged.

- [ ] **Step 10: Commit the target protocol contract**

```bash
git add fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/ApiKeys.java \
  fluss-rpc/src/test/java/org/apache/fluss/rpc/protocol/ApiKeysTest.java \
  fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java \
  fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java \
  fluss-server/src/main/java/org/apache/fluss/server/log \
  fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/main/java/org/apache/fluss/server/index/IndexSender.java \
  fluss-server/src/test/java/org/apache/fluss/server/log/LogTabletTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexSenderTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/tablet/TabletServiceITCase.java
git commit -m "Fence stale index batches"
```

---

### Task 6: Ambiguous Target WAL Append Must Fail-Stop

**Files:**
- Create: `fluss-server/src/main/java/org/apache/fluss/server/kv/UncertainWalAppendException.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/kv/IndexWalAppendFailureTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java`

**Interfaces:**
- Produces: `UncertainWalAppendException extends IOException`
- Produces: package-private `LogTablet.AppendFaultInjector` with deterministic append phases

- [ ] **Step 1: Add phase-specific failure tests**

Inject at `BEFORE_LOCAL_APPEND`, `AFTER_LOCAL_APPEND`, and
`AFTER_WRITER_STATE_UPDATE`. Assert:

- before the LogTablet append call, a known encoding/build failure truncates prewrite and is retryable;
- every exception after the V1 append call begins becomes `UncertainWalAppendException`;
- uncertain paths do not call the normal error truncation;
- `Replica` invokes its fatal error handler and returns no success;
- restart plus retry converges KV and WriterState to the source mutation.

```java
@Test
void testFailureAfterLocalAppendFailStopsReplica() {
    faultInjector.failAt(AFTER_LOCAL_APPEND);
    assertThatThrownBy(
                    () -> putFenced(new WriterKey(7L, Long.MIN_VALUE | 3L), 100L))
            .isInstanceOf(UncertainWalAppendException.class);
    assertThat(fatalErrors).hasSize(1);
    assertThat(replicaAcceptsWrites()).isFalse();
}
```

- [ ] **Step 2: Run the new tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=IndexWalAppendFailureTest,ReplicaTest test
```

- [ ] **Step 3: Mark the exact uncertainty boundary**

Build target WAL before setting the flag. Set `appendInvoked` immediately before
`logTablet.appendAsLeader`:

```java
MemoryLogRecords wal = walBuilder.build();
boolean appendInvoked = false;
try {
    appendInvoked = true;
    return logTablet.appendAsLeader(wal);
} catch (Throwable failure) {
    if (kvRecords.idempotenceProtocolVersion() == 1 && appendInvoked) {
        throw new UncertainWalAppendException(tableBucket, failure);
    }
    kvPreWriteBuffer.truncateTo(previousLogEnd, TruncateReason.ERROR);
    throw failure;
}
```

If the caught value is an `Error`, invoke the fatal path and rethrow the `Error`; wrap
checked/runtime `Exception` in `UncertainWalAppendException`. Do not truncate V1
prewrite on the uncertain branch.

- [ ] **Step 4: Add deterministic LogTablet fault phases**

The default injector is a no-op. Tests install one through a package-private constructor.
Invoke it immediately before `localLog.append`, immediately after `localLog.append`, and
immediately after WriterState update. Do not expose the injector in public APIs.

- [ ] **Step 5: Run tests**

Run the Step 2 command. Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/kv/UncertainWalAppendException.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/test/java/org/apache/fluss/server/kv/IndexWalAppendFailureTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaTest.java
git commit -m "Fail stop uncertain index appends"
```

---

### Task 7: Physical Index Schema And Mutation Encoding

**Files:**
- Modify: `fluss-common/src/main/java/org/apache/fluss/utils/IndexTableUtils.java`
- Modify: `fluss-common/src/test/java/org/apache/fluss/utils/IndexTableUtilsTest.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexTableDescriptorFactory.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexSpec.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexSpecFactory.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java`
- Delete: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexEntryVisibilityFilter.java`
- Delete: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexEntryVisibilityFilterTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexTableDescriptorFactoryTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSpecFactoryTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java`

**Interfaces:**
- Produces: `IndexSpec.IndexEntry IndexSpec.encodeEntry(InternalRow row)`
- Produces: `IndexEntry.key()`, `IndexEntry.value()`, and `IndexEntry.targetBucket()`
- Consumes: `TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION` from Task 1

- [ ] **Step 1: Replace row-version assertions with physical schema assertions**

For a partitioned source, assert exact columns and PK:

```java
assertThat(descriptor.getSchema().getColumnNames())
        .containsExactly("idx", "partition_key", "base_id", "__partition_id");
assertThat(descriptor.getSchema().getPrimaryKeyColumnNames())
        .containsExactly("idx", "partition_key", "base_id", "__partition_id");
assertThat(descriptor.getProperties())
        .doesNotContainKeys(
                ConfigOptions.TABLE_MERGE_ENGINE.key(),
                ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key(),
                ConfigOptions.TABLE_DELETE_BEHAVIOR.key());
assertThat(descriptor.getProperties())
        .containsEntry(
                ConfigOptions.TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION.key(), "1");
```

Add mutation tests asserting old-key changes emit a null-value DELETE and new-key UPSERT,
including a same-target-bucket update where delete precedes upsert.

- [ ] **Step 2: Run index encoding tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=IndexTableDescriptorFactoryTest,IndexSpecFactoryTest,IndexReplicatorAppendTest test
```

- [ ] **Step 3: Derive the physical schema and immutable metadata**

Remove source-offset/deleted columns and their reserved names. Append `__partition_id` to
the primary-key list only for partitioned sources. Explicitly set
`TABLE_KV_IDEMPOTENCE_PROTOCOL_VERSION` to 1 in `IndexTableDescriptorFactory`; do not
infer V1 later from `TABLE_TYPE`. Keep the existing index-target bucket-count derivation,
but do not persist source bucket count as writer metadata.

Do not remove generic `VersionedRowMerger` or its tests; only remove its Index Table
configuration.

- [ ] **Step 4: Encode one complete physical entry**

Replace separate source-row key/value calls with one encoder result:

```java
static final class IndexEntry {
    private final byte[] key;
    private final BinaryRow value;
    private final int targetBucket;
    IndexEntry(byte[] key, BinaryRow value, int targetBucket) {
        this.key = checkNotNull(key, "key");
        this.value = checkNotNull(value, "value");
        this.targetBucket = targetBucket;
    }
    byte[] key() {
        return key;
    }
    BinaryRow value() {
        return value;
    }
    int targetBucket() {
        return targetBucket;
    }
}
```

`IndexSpecFactory` projects index columns plus deduplicated base PK into an Index Table
row; for partitioned input it appends the source replica's constant partition ID. Build
the physical key from the Index Table PK row, not by concatenating bytes manually. The
bucket hash still uses only index columns.

- [ ] **Step 5: Emit physical operations and remove visibility code**

```java
if (oldEntry.hasIndexColumns && keysDiffer) {
    getBuilder(oldEntry.targetBucket).appendDelete(oldEntry.key);
}
if (newEntry != null && (!oldEntry.hasIndexColumns || keysDiffer)) {
    getBuilder(newEntry.targetBucket()).appendUpsert(newEntry.key(), newEntry.value());
}
```

Remove all `IndexEntryVisibilityFilter` installation and lookup calls. Keep the partition
tombstone query filter. Delete tests that only prove logical-delete visibility.

- [ ] **Step 6: Run focused tests**

Run the Step 2 command. Expected: PASS with strong byte-level key/value assertions.

- [ ] **Step 7: Commit**

```bash
git add -A fluss-common/src/main/java/org/apache/fluss/utils/IndexTableUtils.java \
  fluss-common/src/test/java/org/apache/fluss/utils/IndexTableUtilsTest.java \
  fluss-server/src/main/java/org/apache/fluss/server/index \
  fluss-server/src/test/java/org/apache/fluss/server/index
git commit -m "Use physical index mutations"
```

---

### Task 8: Source Windows And Protocol V1 IndexSender Batches

**Files:**
- Modify: `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexWindow.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexBatch.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexSender.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexReplicatorAppendTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexWindowTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSenderTest.java`

**Interfaces:**
- Produces: one V1 batch per `(WriterKey, windowEndOffset, targetBucket)`
- Produces: source mutation groups that never span polls
- Consumes: `IndexWriterKey.encode`, `FencedKvRecordBatchBuilder`, physical `IndexEntry`

- [ ] **Step 1: Add source-boundary tests**

Add tests for:

- `UPDATE_BEFORE` + adjacent `UPDATE_AFTER` in one source batch succeeds;
- missing, non-adjacent, and cross-batch UPDATE halves fail without advancing;
- resume in the middle of a source batch skips records below the requested offset;
- old and new leaders may choose different window ends;
- two mutations to one target share exactly one V1 batch and sequence;
- one mutation group is never split, even when it crosses the preferred payload bound;
- a single request above Netty's hard limit reports record-too-large once and does not enter retry.

```java
assertThat(window.windowEndOffset()).isEqualTo(expectedExclusiveOffset);
assertThat(decodedBatch.fencedWriterKey()).isEqualTo(IndexWriterKey.encode(sourceBucket));
assertThat(decodedBatch.fencedSequence()).isEqualTo(window.windowEndOffset());
assertThat(decodedBatch.getRecordCount()).isEqualTo(2);
```

- [ ] **Step 2: Run focused tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=IndexReplicatorAppendTest,IndexWindowTest,IndexSenderTest test
```

- [ ] **Step 3: Remove cross-poll UPDATE state**

Delete `PendingUpdateBefore`. Process a source `LogRecordBatch` as complete mutation
groups. Throw a typed corruption exception when FULL changelog violates adjacency or
batch completeness. Never assign a new pushed offset after consuming only
`UPDATE_BEFORE`.

When `LogTablet.read(P)` returns a batch whose base is below `P`, skip individual records
with `record.logOffset() < P` before constructing groups.

- [ ] **Step 4: Cut windows by derived output, then attach writer state**

Before adding a group, use builder size deltas for all affected target buckets. End a
non-empty window before a group that crosses the preferred payload. Finalization is:

```java
BytesView finish(WriterKey writerKey, long windowEndOffset) throws IOException {
    builder.setWriterState(writerKey, windowEndOffset);
    return builder.build();
}
```

Create the `IndexWindow` before publishing any batch, set `inFlightWindow` before
accumulator append, and use the same exclusive end for every target batch.

- [ ] **Step 5: Enforce exact serialized request size**

Keep `index.replication.max-request-bytes` as a preferred aggregate payload limit and fix
its description so it does not claim an individual batch can be split. Before send, use
the built PutKv request's serialized size against `netty.server.max-request-size`. An
oversized singleton completes its window with a deterministic failure state and metric;
it is not re-enqueued.

- [ ] **Step 6: Run focused tests**

Run the Step 2 command. Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java \
  fluss-server/src/main/java/org/apache/fluss/server/index \
  fluss-server/src/test/java/org/apache/fluss/server/index
git commit -m "Fence index replication windows"
```

---

### Task 9: Tombstone Readiness, Partition Validation, And Writer Retirement

**Files:**
- Create: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvWriteGuard.java`
- Create: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexKvWriteGuard.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/metadata/TabletServerMetadataCache.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessor.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/delay/DelayedWrite.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/TombstonedPartitionDiscriminator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogAppendInfo.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/WriterStateManager.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/metadata/TabletServerMetadataCacheTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/TombstonedPartitionDiscriminatorTest.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPartitionFenceTest.java`

**Interfaces:**
- Produces: `Optional<PartitionTombstone> TabletServerMetadataCache.getInitializedPartitionTombstone(long)`
- Produces: explicit `removePartitionTombstone(long)` for table deletion
- Produces: `KvWriteGuard.Decision { APPLY, NO_OP }`
- Produces: `Decision KvWriteGuard.beforeWriterState(WriterKey)` and `void KvWriteGuard.validateRecord(WriterKey, byte[], @Nullable BinaryRow)`
- Produces: `IndexKvWriteGuard` installed only for Index Table KvTablet instances
- Consumes: `IndexWriterKey.decode` and `WriterStateManager.removeFencedWriters`
- Produces: `LogAppendInfo.noAppend()` and immediate-success handling in delayed write

- [ ] **Step 1: Add unknown-vs-empty and race tests**

```java
@Test
void testAuthoritativeEmptyIsInitialized() {
    assertThat(cache.getInitializedPartitionTombstone(8L)).isEmpty();
    cache.updatePartitionTombstone(8L, PartitionTombstone.EMPTY);
    assertThat(cache.getInitializedPartitionTombstone(8L))
            .contains(PartitionTombstone.EMPTY);
}

@Test
void testUninitializedPartitionedIndexRejectsBeforePrewrite() {
    assertThatThrownBy(() -> putPartitionedV1(writerKeyForPid(10), 100L))
            .isInstanceOf(StaleMetadataException.class);
    assertNoKvWalOrWriterStateChange();
}
```

Use latches to execute both race orders: apply holds `kvLock` before tombstone publication,
and tombstone publication/retirement acquires it before apply. Assert the first row becomes
query-invisible and the second apply becomes a no-op; both end with no pid-10 WriterState.

- [ ] **Step 2: Run tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=TabletServerMetadataCacheTest,TombstonedPartitionDiscriminatorTest,IndexPartitionFenceTest test
```

- [ ] **Step 3: Preserve authoritative empty baseline**

`updatePartitionTombstone(tableId, EMPTY)` stores EMPTY instead of removing it.
Coordinator `collectPartitionTombstonesForIndexedTables()` includes every partitioned
indexed table, including EMPTY. Table deletion uses the explicit remove method.

Query and compaction paths use `optional.orElse(EMPTY)` for fail-open behavior. V1 write
apply requires the Optional to be present.

- [ ] **Step 4: Add the generic guard boundary and index-owned validation**

Define the common interface without any index type:

```java
public interface KvWriteGuard {
    enum Decision {
        APPLY,
        NO_OP
    }

    Decision beforeWriterState(WriterKey writerKey) throws Exception;

    void validateRecord(
            WriterKey writerKey, byte[] key, @Nullable BinaryRow value) throws Exception;

    KvWriteGuard ACCEPT_ALL = new KvWriteGuard() {
        @Override
        public Decision beforeWriterState(WriterKey writerKey) {
            return Decision.APPLY;
        }

        @Override
        public void validateRecord(
                WriterKey writerKey, byte[] key, @Nullable BinaryRow value) {}
    };
}
```

KvTablet invokes `beforeWriterState` only for V1, under `kvLock`, before WriterState
lookup. It invokes `validateRecord` only on the fresh path after decoding each record and
before applying that record to prewrite. `ReplicaIndexController` creates and injects
`IndexKvWriteGuard` during Index Table KvTablet construction; V0 tables use `ACCEPT_ALL`.

- [ ] **Step 5: Validate WriterKey-derived, key, and value partition IDs**

Under `kvLock`, before WriterState lookup:

```java
IndexWriterKey.SourceBucket source = IndexWriterKey.decode(writerKey);
long expectedPid = source.getPartitionId().getAsLong();
PartitionTombstone tombstone = metadataCache
        .getInitializedPartitionTombstone(mainTableId)
        .orElseThrow(() -> new StaleMetadataException(
                "Partition tombstone baseline is not initialized for " + mainTableId));
if (tombstone.isTombstoned(expectedPid)) {
    return KvWriteGuard.Decision.NO_OP;
}
return KvWriteGuard.Decision.APPLY;
```

For an unpartitioned Index Table, require that the decoded WriterKey has no partition ID
and return APPLY without consulting tombstones. For a partitioned Index Table, require a
partition ID before the snippet above. This mode check and canonical WriterKey decode
must happen even when the current tombstone set is empty.

For UPSERT, compare expected pid with both the structured Index Table row and the last
field decoded from the physical key. `ValueEncoder` derives the v3 value tag from that
same validated row field; assert the resulting encoded tag in `IndexPartitionFenceTest`.
For DELETE, decode the physical key with `KeyDecoder` and compare its last PK field. Do
not compare raw byte suffixes.

- [ ] **Step 6: Serialize retirement with apply**

After cache update, `ReplicaManager` identifies local Index Table replicas whose main
table ID appears in the tombstone update. Each replica executes under its KvTablet guarded
executor, then calls:

```java
logTablet.removeFencedWriters(
        writerKey -> {
            IndexWriterKey.SourceBucket source = IndexWriterKey.decode(writerKey);
            return tombstone.isTombstoned(source.getPartitionId().getAsLong());
        });
```

The method takes LogTablet lock after `kvLock`, preserving the established order. A
tombstoned request returns `LogAppendInfo.noAppend()` before WriterState lookup. The
Replica/DelayedWrite path treats `noAppend` as immediate success with no HW wait, no WAL,
and no WriterState creation.

- [ ] **Step 7: Run tests**

Run the Step 2 command. Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/metadata/TabletServerMetadataCache.java \
  fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorEventProcessor.java \
  fluss-server/src/main/java/org/apache/fluss/server/replica \
  fluss-server/src/main/java/org/apache/fluss/server/index \
  fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java \
  fluss-server/src/main/java/org/apache/fluss/server/kv/KvWriteGuard.java \
  fluss-server/src/main/java/org/apache/fluss/server/log \
  fluss-server/src/test/java/org/apache/fluss/server/metadata/TabletServerMetadataCacheTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index
git commit -m "Fence dropped index partitions"
```

---

### Task 10: Committed Source Retention And Remote Raw-WAL Reader

**Files:**
- Keep and complete: `fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/CompletedSnapshot.java`
- Keep and complete: `fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/CompletedSnapshotJsonSerde.java`
- Keep and complete: `fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/TabletState.java`
- Keep and complete: `fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot/KvTabletSnapshotTarget.java`
- Keep and complete: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/remote/RemoteLogTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/remote/LogTieringTask.java`
- Create: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexSourceReader.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/IndexReplicator.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/index/ReplicaIndexController.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot/KvTabletSnapshotTargetTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogTTLTest.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceReaderTest.java`

**Interfaces:**
- Produces: `long CompletedSnapshot.getMinRetainLogOffset()`
- Produces: `RemoteLogTablet.expiredRemoteLogSegments(long now, Long lakeEnd, long committedMinRetainOffset)`
- Produces: `CompletableFuture<IndexSourceReader.ReadResult> IndexSourceReader.read(long nextOffset, long highWatermark, int maxBytes)`
- Produces: `IndexSourceReader.ReadResult implements AutoCloseable` with `List<LogRecordBatch> batches()` and `long nextOffset()`

- [ ] **Step 1: Add snapshot failure and remote TTL tests**

Use a manual committer that fails upload and commit. Advance volatile index progress to
500 and assert LogTablet's min-retain offset stays at the prior committed value. Commit a
later snapshot and assert it advances exactly once.

For remote segments `[0,10)`, `[10,20)`, `[20,30)`, expired by time with committed floor
15, assert only `[0,10)` is returned. Repeat with lake end below 10 and assert none.

- [ ] **Step 2: Add remote reader continuity tests**

Cover exact `[0,10) -> [10,20) -> local[20,highWatermark)`, an overlapping remote segment, a gap at
10, corrupt remote bytes, remote end before local start, and records at/above HW. Assert
the reader emits each source offset once and fails closed on every gap.

Use a controllable executor to prove a pending remote fetch does not block a second local
IndexReplicator assigned to the same shared read worker.

- [ ] **Step 3: Run focused tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=KvTabletSnapshotTargetTest,CompletedSnapshotJsonSerdeTest,RemoteLogTTLTest,IndexSourceReaderTest test
```

- [ ] **Step 4: Make committed snapshot the only deletion authority**

Keep `Replica#augmentTabletState` capturing the current all-index value, but retain the
ordering already established in `KvTabletSnapshotTarget`: update latest snapshot fields
and invoke `logTablet.updateMinRetainOffset(snapshot.getMinRetainLogOffset())` only after
snapshot commit succeeds. No volatile progress callback calls `updateMinRetainOffset`.

Pass the same committed min-retain offset to remote expiry:

```java
if (segment.remoteLogEndOffset() <= committedMinRetainOffset
        && ttlExpired
        && (lakeEnd == null || segment.remoteLogEndOffset() <= lakeEnd)) {
    expired.add(segment);
}
```

- [ ] **Step 5: Implement non-blocking local/remote source reading**

`IndexSourceReader` owns at most one remote fetch future. Local reads return an already
completed future. When `nextOffset < localLogStartOffset`, submit `RemoteLogFetcher.fetch`
to the dedicated remote-log executor and return that future. `IndexReplicator` stores the
future, returns from `poll()` while it is incomplete, and consumes it with `getNow` only
after completion.
Validate every batch/record next offset, skip overlap below expected offset, and require
the remote/local handoff to equal the next expected offset.

`IndexReplicator.poll()` returns without occupying its worker while the read is pending.
Closing the replicator cancels the future and closes downloaded resources.

- [ ] **Step 6: Run focused tests**

Run the Step 3 command. Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/kv/snapshot \
  fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java \
  fluss-server/src/main/java/org/apache/fluss/server/log/remote \
  fluss-server/src/main/java/org/apache/fluss/server/index \
  fluss-server/src/test/java/org/apache/fluss/server/kv/snapshot \
  fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogTTLTest.java \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexSourceReaderTest.java
git commit -m "Retain source WAL for index replay"
```

---

### Task 11: Target Joint Recovery And Tiering Coverage

**Files:**
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/WriterStateManager.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogTablet.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/LogLoader.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/log/remote/LogTieringTask.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/fetcher/ReplicaFetcherThread.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/log/WriterStateManagerTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/log/LogLoaderTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/log/remote/RemoteLogMaxUploadSegmentsTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/replica/fetcher/ReplicaFetcherThreadTest.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexTargetRecoveryITCase.java`

**Interfaces:**
- Produces: `WriterStateManager.validateRecoveryCoverage(long logStart, long recoveryEnd)`
- Produces: V1 segment upload requires writer snapshot at exclusive segment end
- Consumes: committed KV snapshot min-retain offset from Task 10

- [ ] **Step 1: Add fail-closed recovery tests**

Create target histories with KV snapshot offset `K`, WriterState snapshot offset `R`, and
retained WAL ranges. Assert success for continuous `[K,end)` and `[R,end)`. Assert failure
for:

- no V1 WriterState snapshot with `logStart > 0`;
- corrupt latest snapshot and an older snapshot whose replay range has a gap;
- snapshot after truncation target;
- clean shutdown with no V1 snapshot;
- remote Index Table segment without writer snapshot;
- `ReplicaFetcherThread` writer snapshot download/parse failure.

Also assert fallback succeeds when an older valid snapshot has continuous WAL to the
recovery end.

- [ ] **Step 2: Run tests and verify red**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest=WriterStateManagerTest,LogLoaderTest,RemoteLogMaxUploadSegmentsTest,ReplicaFetcherThreadTest,IndexTargetRecoveryITCase test
```

- [ ] **Step 3: Disable unsafe internal recovery shortcuts**

In `LogTablet.rebuildWriterState`, the clean-shutdown empty-snapshot branch is allowed
only for `V0_COMPACT`. For `V1_FENCED`, choose a valid snapshot `R <= recoveryEnd`
and require either `R >= logStart` or continuous remote/local WAL from R to log start.
If no snapshot exists, scanning from zero is allowed only when retained WAL begins at zero.

Do not remove a corrupt snapshot and continue with unknown V1 writers unless the
next candidate passes the same coverage check.

- [ ] **Step 4: Require WriterState snapshot in Index Table tiering**

For each closed segment ending at exclusive offset `E`:

```java
Path writerSnapshot = log.writerStateManager().fetchSnapshot(E)
        .map(File::toPath)
        .orElseThrow(() -> new LogStorageException(
                "Missing V1 writer snapshot at " + E));
```

Every protocol-V1 KV table requires this path; V0 and Log Tables keep current tiering. A
copy or manifest commit failure leaves
`remoteLogEndOffset` unchanged and makes the segment ineligible for local deletion.

- [ ] **Step 5: Make remote snapshot restore fatal for Index Tables**

In `ReplicaFetcherThread`, rethrow snapshot download, parse, reload, or coverage failures
for a V1 target. Keep existing V0 and Log Table tolerance unchanged. The follower
must not advance its fetch offset or become leader after failure.

- [ ] **Step 6: Verify joint recovery IT**

The IT creates an Index Table, writes a sparse sequence, commits a KV snapshot, rolls and
tiers target WAL, deletes eligible local segments, restarts on a new replica, then sends a
delayed smaller sequence. Assert the delayed batch is stale, target WAL does not grow, and
KV equals the pre-restart state.

- [ ] **Step 7: Run focused tests**

Run the Step 2 command. Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add fluss-server/src/main/java/org/apache/fluss/server/log \
  fluss-server/src/main/java/org/apache/fluss/server/replica \
  fluss-server/src/test/java/org/apache/fluss/server/log \
  fluss-server/src/test/java/org/apache/fluss/server/replica \
  fluss-server/src/test/java/org/apache/fluss/server/index/IndexTargetRecoveryITCase.java
git commit -m "Harden index target recovery"
```

---

### Task 12: Adversarial Failover, Partition Lifecycle, Metrics, And Capacity Gate

**Files:**
- Create: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushOrderingITCase.java`
- Create: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushModelTest.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushFailoverITCase.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/IndexPushReplicationITCase.java`
- Modify: `fluss-server/src/test/java/org/apache/fluss/server/index/PartitionTTLGoldenPathITCase.java`
- Modify: `fluss-common/src/main/java/org/apache/fluss/metrics/MetricNames.java`
- Modify: `fluss-server/src/main/java/org/apache/fluss/server/metrics/group/TabletServerMetricGroup.java`
- Create: `fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexWriterStateBenchmark.java`
- Verify: `docs/superpowers/specs/2026-07-10-index-push-offset-fencing-design.md`
- Modify: `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md`

**Interfaces:**
- Consumes: the complete offset-fenced protocol
- Produces: production gate evidence and final documentation consistency

- [ ] **Step 1: Add old-leader/new-leader ordering IT**

Use a target-side latch to hold old leader requests. Fail over the source leader, let the
new leader restore a conservative snapshot and send differently sized windows, then
release old requests in reverse order. Include:

- UPSERT -> DELETE;
- DELETE -> UPSERT;
- same-key UPSERT -> DELETE -> UPSERT;
- index-key change across two target buckets;
- lost response and retry;
- target leader failover while the dominating sequence is below HW.

Final assertions compare every index entry to a reference projection of committed source
WAL and assert target WAL did not grow for stale requests.

- [ ] **Step 2: Add deterministic model-based test**

For seeds `0..199`, generate 200 source operations and delivery events. The model state is
a map keyed by physical index key; the system-under-test state uses the target apply
state machine. Randomly duplicate, drop responses, reorder, change window boundaries,
truncate uncommitted target WAL, restart, and drop/recreate partitions. After redelivery
settles:

```java
assertThat(actualIndexRows).as("seed=%s", seed).isEqualTo(referenceRows);
assertThat(staleKvMutationCount).isZero();
assertThat(staleWalAppendCount).isZero();
```

- [ ] **Step 3: Strengthen partition lifecycle IT**

Write real rows to pid 10, assert the physical index key and v3 value tag both contain 10,
drop it, recreate the logical partition as pid 20, and release delayed pid-10 UPSERT and
DELETE. Assert pid-20 row survives, pid-10 lookup is filtered immediately, pid-10 writer
state is retired, and compaction physically removes the old row.

- [ ] **Step 4: Remove fixed sleeps and weak assertions**

```bash
rg -n "Thread\.sleep|sleep\(" fluss-server/src/test/java/org/apache/fluss/server/index
```

Replace each synchronization sleep with `waitUntil`, a latch, or a manual clock. Keep a
sleep only when elapsed time itself is the behavior under test and add a comment naming
that timing contract. Replace non-null/count-only assertions with exact offsets, rows,
WriterState sequences, target WAL sizes, and failure types.

- [ ] **Step 5: Add metrics**

Register and assert:

- stale V1 batches;
- V1 WriterState entry count and snapshot bytes;
- source remote-read bytes and failures;
- record-too-large failures;
- tombstone no-op batches; and
- recovery coverage failures.

Do not add a configurable stale threshold or max-Hop2 setting in this task.

- [ ] **Step 6: Add JMH capacity benchmark**

`IndexWriterStateBenchmark` has protocol parameters `{V0_COMPACT,V1_FENCED}`, source
writers `{64,1024,16384}`, and target buckets `{1,16,128}`. For both protocols benchmark
fresh append validation, snapshot serialization, and snapshot reload; for V1 also
benchmark stale fence lookup. Record operations/sec, allocation rate, retained heap,
snapshot bytes, and recovery time. Assert V0 still uses `Map<Long,...>` and its existing
snapshot representation. The V1 setup uses one latest metadata entry per WriterKey and
does not benchmark the rejected logical-row format as production code.

- [ ] **Step 7: Run the full focused index suite**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-server -am \
  -Dspotless.check.skip=true \
  -Dtest='Index*Test,Index*ITCase,PartitionTTLGoldenPathITCase,CompactionFilterITCase,RemoteLogTTLTest,ReplicaFetcherThreadTest' test
```

Expected: PASS with no fixed-sleep synchronization.

- [ ] **Step 8: Compile and run the capacity benchmark**

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-jmh -am \
  -Dspotless.check.skip=true -DskipTests package

mvn -o -Dmaven.repo.local=.cache -pl fluss-jmh \
  dependency:build-classpath -Dmdep.outputFile=target/jmh.classpath

java -cp "fluss-jmh/target/test-classes:fluss-jmh/target/classes:$(cat fluss-jmh/target/jmh.classpath)" \
  org.openjdk.jmh.Main '.*IndexWriterStateBenchmark.*' \
  -wi 3 -i 5 -f 1 -rf json -rff fluss-jmh/target/index-writer-state-result.json
```

Expected: BUILD SUCCESS and a JSON result containing every writer/target-bucket parameter
combination. Record heap, snapshot bytes, operation throughput, and recovery duration in
the execution review; do not infer production capacity from a compile-only result.

- [ ] **Step 9: Update FIP and verify documentation against code names**

Update the FIP to state all of the following exact contracts:

- `table.kv.idempotence-protocol-version` defaults to 0 unless explicitly set;
- IndexTableDescriptorFactory explicitly writes version 1;
- V0 keeps writerId:int64, sequence:int32, current WriterState/WAL/snapshot/TTL;
- V1 uses opaque WriterKey:128bit, sequence:int64, latest fence, explicit retirement;
- PutKv API v2 is capability only and cannot select or downgrade table protocol;
- Log Tables and ProduceLog remain outside V1; and
- the index module alone canonically encodes source partition/bucket into WriterKey.

```bash
rg -n "indexWriterIdBase|sourceEndOffset|source-bucket-count|IndexWriterId|CONTIGUOUS_INT|MONOTONIC_LONG|table.kv.idempotence-mode|table.writer.idempotence|__source_offset|__index_deleted|inflightChunks|PK 幂等性吸收" \
  docs/superpowers/specs/2026-07-10-index-push-offset-fencing-design.md \
  /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md
```

Expected: only explicit rejected-alternative/removal references to the two deleted columns;
no long writer mapping, source bucket count, behavior-only mode, assignment writer range,
separate source-end field, or PK-only recovery claim. Confirm both documents contain
`table.kv.idempotence-protocol-version`, `V0_COMPACT`, `V1_FENCED`, `WriterKey`,
`PutKv API v2`, and `ProduceLog`.

- [ ] **Step 10: Run repository verification**

```bash
mvn clean compile -o -Dmaven.repo.local=.cache \
  -pl '!fluss-lake/fluss-lake-lance,!fluss-dist' \
  -Dspotless.check.skip=true

mvn test -o -Dmaven.repo.local=.cache \
  -pl '!fluss-lake/fluss-lake-lance,!fluss-dist' \
  -Dspotless.check.skip=true

git diff --check
```

If offline resolution alone fails, rerun the same Maven command once without `-o` to fill
`.cache`, then repeat offline. Expected: both offline commands end in BUILD SUCCESS and
`git diff --check` is silent.

- [ ] **Step 11: Commit the production gate**

```bash
git add fluss-common/src/main/java/org/apache/fluss/metrics/MetricNames.java \
  fluss-server/src/main/java/org/apache/fluss/server/metrics/group/TabletServerMetricGroup.java \
  fluss-server/src/test/java/org/apache/fluss/server/index \
  fluss-jmh/src/test/java/org/apache/fluss/jmh/IndexWriterStateBenchmark.java \
  docs/superpowers/specs/2026-07-10-index-push-offset-fencing-design.md \
  docs/superpowers/plans/2026-07-10-index-push-offset-fencing.md
git commit -m "Verify offset fenced index push"
```

Do not declare production-ready until the benchmark result for the supported maximum
topology has been recorded and reviewed. Passing functional tests proves semantics, not
capacity.

---

## Final Review Checklist

- [ ] Every KV table defaults to protocol V0 unless the property is explicitly set.
- [ ] V0 KV/WAL/snapshot bytes, `Map<Long,...>`, expiration, and exact-next tests are unchanged.
- [ ] V1 KV/WAL sequences and full WriterKeys round-trip above `Integer.MAX_VALUE`.
- [ ] PutKv API/table protocol/batch magic matrix passes with no automatic fallback.
- [ ] Log Tables and ProduceLog cannot create or consume V1 writer state.
- [ ] No assignment metadata or leader RPC contains index writer identity.
- [ ] No Index Table row contains source offset or logical-delete state.
- [ ] Stale requests do not decode, mutate KV, append WAL, or acknowledge before dominating HW.
- [ ] Source snapshot commit failure cannot release local or remote WAL.
- [ ] Target recovery cannot continue with missing WriterState coverage.
- [ ] Partition unknown/empty states are distinct and drop/recreate is incarnation-safe.
- [ ] Ambiguous append tests prove fail-stop and convergent recovery.
- [ ] Model-based and failover ITs compare exact final state to committed source WAL.
- [ ] No fixed-sleep synchronization remains in the index suite.
- [ ] Full offline compile and test commands pass.
- [ ] Capacity evidence is reviewed before production-ready status.
