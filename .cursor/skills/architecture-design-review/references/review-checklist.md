# Design Review Checklist (Fluss dynamic bucket)

## Per scheme document — minimum bar

Each scheme doc (especially production path) should include:

- [ ] Problem statement + non-goals
- [ ] Applicability matrix (partitioned / non-partitioned / lake-enabled)
- [ ] End-to-end sequence diagram (mermaid)
- [ ] State machine or phase table (if async job)
- [ ] Metadata fields with types and mutation rules
- [ ] RPC/API list with idempotency notes
- [ ] Failure modes + rollback table
- [ ] CDC / Union Read semantics per phase
- [ ] Flink interaction (separate Buckload compute vs business connector)
- [ ] Acceptance criteria (IT cases named)
- [ ] Explicit relationship to other schemes

## Buckload-specific probes

- [ ] fenceSpec: per old bucket snapshot id + log end offset?
- [ ] Merge engine: how are deletes/tombstones represented in cold KV?
- [ ] Bootstrap log offset=0 vs loaded KV: HW/LEO initialization
- [ ] manifest-last commit vs TS polling forbidden
- [ ] RocksDB options same factory as KvTablet
- [ ] Tiering paused: who pauses, how resumes, race with in-flight tier job
- [ ] Shrink N: bucket merge mapping documented

## Hub document probes

- [ ] Scheme index links work
- [ ] No full Buckload spec duplicated (only summary + link)
- [ ] Invariants I1–I7 traceable to at least one scheme
- [ ] Milestones M1–M3 map to concrete deliverables
