---
name: architecture-decision-records
description: Document significant technical decisions as Architecture Decision Records (ADRs) with context, options, and rationale. Use when a design choice is hard to reverse or needs team audit trail (e.g. Buckload vs in-cluster migration, manifest-first remote commit).
paths: docs/decisions/**,website/docs/design/**
disable-model-invocation: true
---

# Architecture Decision Records

Capture **why** a decision was made, not the full design (that stays in scheme docs).

## When to write an ADR

- Hard to reverse (bucketMode, Buckload as default data plane)
- Multiple valid options were debated (manifest-first vs mvdir, KvSnapshot vs custom format)
- Will be questioned in 6+ months

## Template

Create `docs/decisions/NNN-short-title.md`:

```markdown
# ADR-NNN: Title

## Status
Accepted | Proposed | Superseded by ADR-XXX

## Context
Problem, constraints, forces.

## Options
### A: ...
### B: ...

## Decision
We choose **A** because ...

## Consequences
Positive and negative follow-ups.
```

## Fluss dynamic bucket — candidate ADRs

- ADR: Buckload as default vs in-cluster replay
- ADR: manifest-first remote commit (no mvdir)
- ADR: FIXED_HASH main track vs DYNAMIC_INDEX/CH
- ADR: Scheme 2 dual-read not productized

Keep ADRs 1–2 pages. Link from hub doc §14 or scheme docs.
