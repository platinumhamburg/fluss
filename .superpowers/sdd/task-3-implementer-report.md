# Task 3 Implementer Report

## Scope

- Base: `9b69d9403` (clean worktree at start).
- Added target-only WAL magic V3 with a 68-byte fixed header, exactly 12 bytes larger than V2.
- Kept `CURRENT_LOG_MAGIC_VALUE`, ProduceLog/client code, existing factories, and V0-V2 field layouts unchanged.

## RED

Command:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=DefaultLogRecordBatchTest,LogRecordBatchFormatTest,FileLogInputStreamTest,FileLogProjectionTest,ArrowWalBuilderTest test
```

Result: exit 1 during `fluss-common` test compilation. Expected missing symbols included
`LOG_MAGIC_VALUE_V3`, fenced offsets/accessors, `fencedBuilder`,
`setFencedWriterState`, and `WalBuilder` fenced state.

## GREEN

Focused command: the RED command above.

Result: exit 0; 72 tests passed (`69` common + `3` server), 0 failures/errors/skips.

Compatibility command (reactor-required overrides added after the brief's literal command
failed at the empty root module with "No tests were executed"):

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client,fluss-server -am \
  -Dspotless.check.skip=true -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=RecordAccumulatorTest,SenderTest,LogTabletTest test
```

Result: exit 0; 45 tests passed (`17` server + `28` client), 0 failures/errors/skips.

## Fixture Provenance

The V0 and V2 literal empty-batch fixtures were captured before production edits from base
`9b69d9403`'s fixed offsets and writer fields, with JDK `CRC32C` applied over the same
schema-to-end range. The tests compare every byte, including CRC, for writer `7` and sequence
`Integer.MAX_VALUE`. Header-offset tests independently retain all prior V0-V2 constants.

## Files

- Common format/readers: `LogRecordBatch`, `LogRecordBatchFormat`,
  `DefaultLogRecordBatch`, `FileLogInputStream`, `FileLogProjection`.
- Common builders: row, Arrow, compacted, and indexed memory WAL builders.
- Server builders: `WalBuilder`, `ArrowWalBuilder`, `CompactedWalBuilder`, `IndexWalBuilder`.
- Tests: the five Task 3 test files from the brief.

## Self-Review

- V3 extends V2 only through `lastOffsetDelta`, then writes key high/low, long sequence,
  record count, and statistics length at offsets 36/44/52/60/64.
- V0-V2 ordinary and V3 fenced accessors reject cross-protocol use; no byte reinterpretation.
- V3 requires a non-null opaque `WriterKey` and accepts sequences from 0 through
  `Long.MAX_VALUE`; ordinary factories cannot produce V3.
- Memory, file input, projection, and all three server WAL representations round-trip V3.
- File input and projection reject physically or declaratively undersized V3 headers before
  reading V3 fields. Projection retains the legacy 56-byte V0-V2 header-read path.
- Audited every magic comparison in owned sources; `>= V2` remains only for shared leader epoch,
  and `>= V1` only for shared statistics semantics.

## Concerns

- `spotless:apply` is blocked on JDK 24 by the repository's google-java-format integration
  (`NoSuchMethodError` in javac internals). Builds ran with the brief's Spotless skip; checkstyle
  passed and `git diff --check` is clean.
- Task 5 still owns selecting the explicit fenced WAL factories for protocol V1 tables.

---

## Review Fixes (2026-07-11)

### Findings Addressed

- Row WAL fenced-state mutation now invalidates the cached built buffer. Common indexed and
  server compacted/indexed regressions verify replacement key, long sequence, changed CRC, and
  `ensureValid()` after rebuilding.
- Projection validates magic, declared/physical batch bounds, non-negative statistics and record
  counts, checked records/change-type/Arrow offsets, metadata/body/buffer bounds, and header-only
  consistency before schema lookup or version-specific reads. Corruption fixtures include trailing
  bytes that must not be borrowed as a following batch.
- Projection now recomputes CRC for every projected complete batch, including V0-V2 and V3, after
  length/statistics/payload changes.
- Memory-segment and file inputs reject unknown magic, negative/overflow lengths, and
  version-specific undersized headers before returning a batch. File coverage includes exactly the
  12-byte prefix boundary.
- The ordinary Sender path is exercised through a real `ProduceLogRequest`; every emitted batch
  remains below V3 and reports idempotence protocol 0.
- Added non-empty V3 Arrow, compacted, and indexed server WAL round trips plus a non-empty V3 Arrow
  statistics round trip, all checking records, fenced metadata, and CRC validity.

### RED

Six focused RED runs each failed one newly added reproducer (6 failures total): cached row-header
state, projected CRC, projection layout/validation ordering, memory input malformed headers, file
input malformed headers, and the exact-prefix unknown-magic file boundary. Representative commands:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -Dspotless.check.skip=true \
  -Dtest=DefaultLogRecordBatchTest#testRowBuilderRewritesBuiltHeaderAfterFencedStateChanges test
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -Dspotless.check.skip=true \
  -Dtest=FileLogProjectionTest#testV3WriterKeyAndLongSequenceSurviveProjection test
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -Dspotless.check.skip=true \
  -Dtest=FileLogProjectionTest#testProjectionRejectsMalformedLayoutBeforeReadingFollowingBatchBytes test
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -Dspotless.check.skip=true \
  -Dtest=MemorySegmentLogInputStreamTest#testRejectsMalformedBatchHeadersBeforeReturningBatch test
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -Dspotless.check.skip=true \
  -Dtest=FileLogInputStreamTest#testRejectsUnknownMagicAndInvalidDeclaredLengths test
```

The projection-layout RED reached schema lookup instead of corruption validation; the CRC RED
returned `isValid() == false`. The exact-prefix file case was added during self-review and failed
the same file-input test before the EOF comparison was corrected. The common row-builder RED covers
the shared implementation used by both server row WAL adapters.

### GREEN

Task 3 focused suite:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=DefaultLogRecordBatchTest,LogRecordBatchFormatTest,FileLogInputStreamTest,FileLogProjectionTest,ArrowWalBuilderTest test
```

Result: 77 passed (`72` common + `5` server), 0 failures/errors/skips.

Direct-memory/client suite:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-client -am \
  -Dspotless.check.skip=true -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=MemorySegmentLogInputStreamTest,MemoryLogRecordsArrowBuilderTest,SenderTest test
```

Result: 41 passed (`23` common + `18` client), 0 failures/errors/skips.

Compatibility suite:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client,fluss-server -am \
  -Dspotless.check.skip=true -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=RecordAccumulatorTest,SenderTest,LogTabletTest test
```

Result: 46 passed (`17` server + `29` client), 0 failures/errors/skips.

### Fixture Provenance And Compatibility

The original V0/V2 literal fixtures remain byte-for-byte gates captured from base `9b69d9403` as
documented above. No V0-V2 format constants, ordinary builder defaults, ProduceLog production code,
or client production classes changed. Existing focused and compatibility gates stayed green.

### Performance Note

Projected output still composes headers/metadata with zero-copy file regions for selected Arrow
body buffers. The available `BytesView`/file-region API has no streaming checksum primitive, so CRC
recomputation performs one bounded sequential read over only the projected change-type/body bytes
using a reusable 16 KiB buffer; it does not flatten or copy the full batch. This cost applies
correctly to projected V0-V3 batches. Non-projection V0-V2 builder paths are unchanged, and reader
validation adds only fixed-header checks.

### Files Changed

- `fluss-common/src/main/java/org/apache/fluss/record/FileLogInputStream.java`
- `fluss-common/src/main/java/org/apache/fluss/record/FileLogProjection.java`
- `fluss-common/src/main/java/org/apache/fluss/record/MemoryLogRecordsRowBuilder.java`
- `fluss-common/src/main/java/org/apache/fluss/record/MemorySegmentLogInputStream.java`
- `fluss-common/src/test/java/org/apache/fluss/record/DefaultLogRecordBatchTest.java`
- `fluss-common/src/test/java/org/apache/fluss/record/FileLogInputStreamTest.java`
- `fluss-common/src/test/java/org/apache/fluss/record/FileLogProjectionTest.java`
- `fluss-common/src/test/java/org/apache/fluss/record/MemoryLogRecordsArrowBuilderTest.java`
- `fluss-common/src/test/java/org/apache/fluss/record/MemorySegmentLogInputStreamTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/kv/wal/ArrowWalBuilderTest.java`
- `fluss-client/src/test/java/org/apache/fluss/client/write/SenderTest.java`

### Self-Review And Concerns

- CRC coverage starts at each magic's schema offset, matching `DefaultLogRecordBatch`; the fix is
  deliberately generic rather than V3-special-cased.
- Offset arithmetic uses `long` intermediates and subtraction-based body bounds to avoid overflow.
- Corrupt projection structure is rejected before schema access and before reads can cross the
  declared batch end.
- No open correctness concern remains. Spotless remains skipped for the documented JDK 24 plugin
  incompatibility; reactor checkstyle and the final diff whitespace audit pass.

---

## Tail Compatibility And Arrow Validation Follow-Up (2026-07-11)

### Semantics And Fixes

- All three range readers now use the same declaration/physical split. Fewer than the 12 common
  prefix bytes is recovery EOF. Once the prefix exists, unknown magic and negative, overflowing,
  or version-short declarations are corruption. For recognized V0-V3 with a valid declaration,
  a physically incomplete fixed header or declared batch is recovery EOF; projection returns the
  bytes already accumulated from complete batches unchanged.
- `projectRecordBatch` remains a strict concrete-batch API: missing fixed-header bytes raise EOF and
  a missing declared payload raises `CorruptMessageException`. Its Javadoc and regression test make
  this distinction from range projection explicit.
- Arrow projection rejects any buffer whose end crosses the next buffer start. It also verifies the
  schema-required node and buffer counts before indexed access, producing explicit corruption
  errors while permitting compatible source metadata with additional nodes/buffers.

### RED

Reader reproducer:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -Dspotless.check.skip=true \
  -Dtest=FileLogInputStreamTest,MemorySegmentLogInputStreamTest test
```

Result before the fix: 8 errors, one physically incomplete recognized-header case for each V0-V3
in each reader; all incorrectly threw corruption instead of returning no batch.

Projection reproducer:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -Dspotless.check.skip=true \
  -Dtest=FileLogProjectionTest#testRangeProjectionPreservesOutputBeforeIncompleteTail+testRangeProjectionRejectsInvalidDeclarationFromCommonPrefix+testProjectRecordBatchRetainsStrictFullBatchContract+testProjectionRejectsOverlappingArrowBufferSpans+testProjectionRejectsMissingRequiredArrowNodesAndBuffers test
```

Result before the fix: 10 failing/error invocations in 11 tests: four recognized incomplete tails
were rejected, four malformed declarations were treated as EOF, overlap was accepted, and missing
required metadata reached indexed access. The strict concrete-batch case already passed RED.

### GREEN

Focused reader/projection suite:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common -Dspotless.check.skip=true \
  -Dtest=FileLogInputStreamTest,MemorySegmentLogInputStreamTest,FileLogProjectionTest test
```

Result: 82 passed (`18` file input + `10` memory input + `54` projection), 0 failures/errors/skips.
The direct strict-contract extension also passed independently: 1 test, 0 failures/errors/skips.

Task 3 focused suite:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-common,fluss-server -am \
  -Dspotless.check.skip=true -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=DefaultLogRecordBatchTest,LogRecordBatchFormatTest,FileLogInputStreamTest,FileLogProjectionTest,ArrowWalBuilderTest test
```

Result: 96 passed (`91` common + `5` server), 0 failures/errors/skips.

Compatibility suite:

```bash
mvn -o -Dmaven.repo.local=.cache -pl fluss-client,fluss-server -am \
  -Dspotless.check.skip=true -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=RecordAccumulatorTest,SenderTest,LogTabletTest test
```

Result: 46 passed (`17` server + `29` client), 0 failures/errors/skips.

### Performance, Files, And Self-Review

- Range projection still reads one 56-byte header block for V0-V2 and only extends it by 12 bytes
  for complete V3 headers. No common-prefix reread was introduced. Existing zero-copy body
  composition and bounded streaming projected-CRC calculation are unchanged.
- Changed files: `FileLogInputStream.java`, `MemorySegmentLogInputStream.java`,
  `FileLogProjection.java`, and their three focused test classes.
- Boundary coverage exhausts every physical size from the complete common prefix through one byte
  below each V0-V3 fixed header, plus full-header/incomplete-payload and declaration-corruption
  cases. V0-V2 wire bytes, defaults, ProduceLog, and client production code remain untouched.
- Checkstyle passed in all reactors and `git diff --check` is clean. No open concern remains.
