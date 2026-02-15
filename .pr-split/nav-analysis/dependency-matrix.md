# Dependency Matrix

## Cross-file dependencies within the diff

### Null Count Related (Commit 2)

1. **ArrowNullCountReader.java** (new) ← used by:
   - DefaultLogRecordBatch.java (readAndSetNullCountsFromArrowMetadata)
   - FileLogInputStream.java (readAndSetNullCountsFromArrowMetadata)

2. **LogRecordBatchFormat.java** (STATISTICS_VERSION_V1/V2/CURRENT) ← used by:
   - LogRecordBatchStatisticsParser.java (version checks)
   - LogRecordBatchStatisticsWriter.java (writes CURRENT_STATISTICS_VERSION)

3. **DefaultLogRecordBatchStatistics.java** (setNullCounts, getStatsIndexMapping, mutable statsNullCounts) ← used by:
   - DefaultLogRecordBatch.java (calls setNullCounts after Arrow extraction)
   - FileLogInputStream.java (calls setNullCounts after Arrow extraction)
   - LogRecordBatchStatisticsParser.java (returns DefaultLogRecordBatchStatistics with null nullCounts for V2)

4. **LogRecordBatchStatisticsWriter.java** (removed nullCounts param) ← used by:
   - LogRecordBatchStatisticsCollector.java (calls writeStatistics without nullCounts)

5. **LogRecordBatchStatisticsCollector.java** (removed null count tracking) ← used by:
   - MemoryLogRecordsArrowBuilder (indirectly, via statistics collection)

### Non-Null-Count Related (Commit 1)

6. **DefaultCompletedFetch.java** — standalone fix (Long → long)
7. **LogFetcher.java** — standalone fixes (import cleanup, typo fix)
8. **LogFetcherFilterITCase.java** — standalone test cleanup (remove redundant field, improve assertions)
9. **LogRecordReadContext.java** — standalone cleanup (remove unused method)
10. **FetchParams.java** — standalone cleanup (remove @VisibleForTesting)
11. **FetchParamsBuilder.java** — standalone fix (add @Nullable)
12. **Replica.java** — standalone log message improvements
13. **LocalLogTest.java** — standalone new test
14. **LogSegmentTest.java** — standalone test improvements
