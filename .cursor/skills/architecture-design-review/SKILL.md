---
name: architecture-design-review
description: Challenger review of distributed-system architecture and technical design documents. Use when reviewing RFCs, HLDs, scheme specs, or when the user asks to critique, audit, or improve a design doc. Requires codebase verification and severity-rated findings.
paths: website/docs/design/**,docs/design/**,**/*design*.md
disable-model-invocation: false
---

# Architecture Design Review (Challenger Mode)

Perform a **skeptical, evidence-based** review of technical design documents. Default stance: the design has gaps until proven against code and invariants.

## When to use

- User asks to review, critique, audit, or improve a design doc
- Before marking a design "Architecture Specification" or "Accepted"
- After major doc rewrites (hub + scheme split, new data plane like Buckload)

## Review workflow

### Phase 1 — Scope and claims inventory

1. List every **normative claim** (MUST/SHALL/默认/禁止) in the doc.
2. List every **interface** (RPC, metadata field, config key, state name).
3. List every **invariant** referenced (I1–In) and how the doc claims to satisfy each.
4. Note what is marked **待决 / TBD / 建议** — these are review debt.

### Phase 2 — Codebase verification

For each claim, search the repository:

```bash
rg -n "<symbol>" --glob '*.java'
```

Classify each claim:

| Label | Meaning |
|-------|---------|
| **VERIFIED** | Matches existing code or official user-facing docs |
| **PLANNED** | Explicitly future work; doc should say so |
| **UNVERIFIED** | No code anchor; needs proto sketch or "target design" banner |
| **CONTRADICTS** | Conflicts with code or another section — **blocker** |

Pay special attention to: Tiering, remote storage, Flink connector, coordinator metadata, bucketing, lake tiering.

### Phase 3 — Seven-lens challenger checklist

Rate each lens: **Pass / Gap / Blocker**

1. **Correctness & invariants** — Can I1–I7 hold at every state transition? Race windows?
2. **Failure modes** — Crash mid-BUCKLOADING, duplicate notifyLoadBuckload, partial manifest, TS leader change?
3. **Idempotency & retries** — Every RPC and remote write safe to retry?
4. **Operational semantics** — What does the operator see? How long is FENCE? Rollback story?
5. **Cross-subsystem coupling** — Tiering, Rebalance, CDC, Union Read, Flink business jobs — mutex and ordering?
6. **Data plane completeness** — Read path + write path + install path + verify; KV/Log alignment?
7. **Scope honesty** — Non-partition tables, shrink bucket, lake on/off, multi-N per table?

### Phase 4 — Severity-rated findings

Output findings in this format:

```markdown
### [P0|P1|P2] Short title
- **Where**: file §section
- **Issue**: what is wrong or missing
- **Impact**: what breaks if unfixed
- **Fix**: concrete doc or design change (not vague "add more detail")
```

| Severity | Criteria |
|----------|----------|
| **P0** | Correctness/invariant violation possible in production path |
| **P1** | Implementer cannot build without guessing; ops cannot run |
| **P2** | Clarity, completeness, competitor depth, diagram quality |

### Phase 5 — Deliverables

1. **Review report** — findings table + summary verdict (Ready / Not ready / Ready with P1 debt)
2. **Doc patches** — fix P0/P1 in the same session when possible
3. **Open questions** — numbered list for product/tech lead decision

## Anti-patterns to flag

- Mixing **compute engine parallelism** with **data materialization layout** (e.g. Flink Key Group in Buckload sections)
- **Hub doc duplication** — same Buckload spec in hub and scheme-01 without single source of truth
- **Thin scheme docs** — <200 lines for a production path; missing sequence diagram, failure table, acceptance tests
- **Hand-wavy fence/snapshot** — "fenceSpec" without per-bucket offset structure
- **PK table rescale without tombstone/delete semantics** in VERIFY
- **Industry reference** as one paragraph without structured competitor matrix

## References

Load if present in this skill folder:

- `references/review-checklist.md` — expanded checklist
- `references/fluss-design-context.md` — Fluss-specific anchors (modules, existing APIs)
