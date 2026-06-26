---
name: architecture-design-authoring
description: Write and expand distributed-system architecture and technical scheme documents to production-review quality. Use when authoring HLDs, scheme specs, RFCs, or thickening thin design sections for Fluss or similar systems.
paths: website/docs/design/**,docs/design/**
disable-model-invocation: false
---

# Architecture Design Authoring

Write technical designs that implementers and operators can execute **without oral tradition**.

## Document types

| Type | Purpose | Typical location |
|------|---------|------------------|
| **Hub / overview** | Invariants, selection guide, cross-cutting constraints | `*-design.md` |
| **Scheme spec** | One technical route, fully specified | `scheme-NN-*.md` |
| **Review report** | Challenger findings + remediation | `DESIGN-REVIEW.md` |
| **ADR** | Single decision record | `docs/decisions/NNN-*.md` |

Use `/architecture-design-review` after authoring to validate.

## Hub document structure

1. Executive summary (problem, default track, milestones)
2. Current vs target capability table (**code-verified**)
3. Invariants with acceptance tests
4. Architecture diagram (one page)
5. **Scheme index** — link only; no duplicate deep spec
6. Cross-cutting: lake, bucketing functions, metadata summary
7. Ops runbook (short) + risks/TBD

Keep hub **<500 lines**. Deep content lives in scheme docs.

## Scheme document structure (required sections)

```markdown
## 1. 方案定义 — problem, unit of work, non-goals
## 2. 核心机制 — phases, semantics, diagrams
## 3. 数据面 / 控制面 — read/write/install paths
## 4. 元数据 — fields, when they change
## 5. 湖流一体 — per RescaleJob phase if applicable
## 6. Flink 协同 — Buckload (pure compute) vs business connector (separate!)
## 7. CDC 语义
## 8. 失败模式与回滚
## 9. 优缺点
## 10. 交付与验收 — named IT cases
## 11. 与其他方案的关系
```

Production path (scheme-01) additionally requires:

- Full RPC table with idempotency column
- manifest JSON example (complete logical fields)
- mermaid sequenceDiagram for happy path
- mermaid stateDiagram if job-driven

## Writing rules

1. **Normative language**: 必须 / 禁止 / 默认 / 建议 — be consistent
2. **No false precision**: mark 待决 explicitly; add draft proto/sketch when P1
3. **Separate dimensions**: Fluss bucket layout ≠ Flink job parallelism ≠ Flink Key Group (only in CH/vnode schemes)
4. **Code anchors**: cite real classes/packages when claiming "reuse X"
5. **Diagrams**: at least one sequence + one state/flow per async workflow
6. **Competitor matrix**: table with Product | Mechanism | PK table? | Lake? | Fluss takeaway

## Depth targets

| Doc | Min lines (guide) | Notes |
|-----|-------------------|-------|
| Hub | 300–450 | Thin index |
| Scheme-01 (production) | 350+ | Buckload full spec |
| Scheme-02 (rejected) | 150+ | Why-not + boundary |
| Scheme-03/04 (future) | 200+ | Enough for mode selection |
| Scheme-05 (M1) | 200+ | Metadata + Flink connector |

## After writing

1. Run `/architecture-design-review` on changed files
2. Fix P0/P1 findings before commit
3. Link hub ↔ schemes bidirectionally
