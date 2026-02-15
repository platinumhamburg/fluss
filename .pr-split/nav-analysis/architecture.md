# Architecture Analysis

## Modules
- **fluss-client** (3 files): Client-side log scanner/fetcher. Changes are minor fixes and test cleanup.
- **fluss-common** (14 files): Core record format layer. Contains the bulk of changes — statistics format versioning (V1→V2), Arrow null count extraction, and all related tests.
- **fluss-server** (6 files): Server-side log reading and replica management. Changes are minor fixes and new filter tests.

## Key Abstractions
- **LogRecordBatchStatistics**: Interface for batch-level column statistics (min/max/nullCounts).
- **DefaultLogRecordBatchStatistics**: Implementation that holds parsed statistics data.
- **LogRecordBatchStatisticsWriter**: Serializes statistics to binary format.
- **LogRecordBatchStatisticsParser**: Deserializes statistics from binary format.
- **LogRecordBatchStatisticsCollector**: Collects statistics during record writing.
- **ArrowNullCountReader** (new): Extracts null counts from Arrow FlatBuffer metadata.

## Split Boundary
The split cleanly divides into:
1. **Commit 1 (non-null-count)**: 24 hunks across client, server, and one common file. All independent fixes: type narrowing, import cleanup, typo fix, assertion improvements, test additions, log message improvements, unused code removal.
2. **Commit 2 (null count optimization)**: 51 hunks concentrated in fluss-common. Introduces V2 statistics format that omits null counts from the binary layout and instead extracts them from Arrow RecordBatch FlatBuffer metadata. Includes new ArrowNullCountReader class, format version constants, writer/parser/collector changes, and comprehensive tests.
