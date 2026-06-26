# Fluss design doc anchors (for reviewers)

When verifying claims, search these modules:

| Topic | Module / class |
|-------|----------------|
| Flink Sink numBucket | `fluss-flink-common/.../FlinkTableSink.java` |
| Tiering read path | `fluss-flink-common/.../tiering/` |
| TieringSplitGenerator | `.../tiering/source/split/TieringSplitGenerator.java` |
| KV cold load | `fluss-server/.../KvSnapshotDataDownloader.java` |
| Remote data dir | `fluss-server/.../coordinator/remote/` |
| Coordinator buckets | `fluss-server/.../CoordinatorService.java` |
| Rebalance | server coordinator rebalance packages |

**Not implemented (PLANNED):** RescaleJob, Buckload, layoutEpoch, defaultBucketCount per-partition, buckload RPC.
