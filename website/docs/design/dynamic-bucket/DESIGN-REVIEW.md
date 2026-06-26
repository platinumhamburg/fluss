---
title: "Dynamic Bucket Design — Challenger Review"
sidebar_label: Design Review
sidebar_position: 6
---

# 动态分桶设计文档 — Challenger 评审报告

> **评审方法**：`.cursor/skills/architecture-design-review`  
> **评审对象**：`dynamic-bucket-rescaling-design.md` + `scheme-01`～`scheme-05`  
> **评审日期**：2026-06-26  
> **总体结论**：**Not ready for implementation sign-off** — 主轨方向正确，但方案一生产路径仍有 **P0/P1 规格缺口**；调研与竞品深度不足；方案二～四厚度不均。

---

## 1. 评审摘要

| 维度 | 评分 | 说明 |
|------|------|------|
| 方案选型与主轨 | **良好** | 五方案索引清晰；五+一组合合理；方案二明确不采纳 |
| 跨方案不变量 | **良好** | I1–I7 定义清楚，但部分方案未逐条验收映射 |
| 方案一（生产路径） | **不足** | Buckload 骨架有，缺 fenceSpec 结构、KV/Log 对齐、缩桶、失败模式表 |
| 方案五（M1） | **尚可** | 元数据语义清楚；Flink 并行度表述易误导 |
| 方案二 | **合格（边界档）** | 作为「为何不选」够用 |
| 方案三/四 | **偏薄** | 预研档可接受，但缺 PoC 验收与迁移时序图 |
| 行业调研 | **不足** | 主文档 §13 一段带过，无结构化竞品矩阵 |
| 与代码对齐 | **部分** | `FlinkTableSink.numBucket` 规划期固化已验证；RescaleJob/Buckload **均未实现** |

---

## 2. 代码核实（抽样）

| 文档声称 | 核实结果 | 锚点 |
|----------|----------|------|
| `bucket.num` 建表后 ALTER 不可改 | **VERIFIED** | 用户文档 + 表属性模型 |
| `FlinkTableSink` 规划期固化 `numBucket` | **VERIFIED** | `FlinkTableSink` 构造参数 `numBucket` |
| Tiering 外置 Flink + `TieringSplitGenerator` | **VERIFIED** | `fluss-flink-tiering` 模块 |
| `KvSnapshotDataDownloader` 可复用冷加载 | **VERIFIED** | `fluss-server` KV snapshot 路径 |
| `remoteDataDir` / `RemoteDirSelector` | **VERIFIED** | `RoundRobinRemoteDirSelector` 等 |
| RescaleJob / Buckload / `layoutEpoch` | **PLANNED** | 代码库无实现 — 文档须标 **目标设计** |
| `defaultBucketCount` per-partition | **PLANNED** | M1 目标，当前表级 `numBuckets` |

---

## 3. 分级发现

### P0 — 实现前必须闭合

#### P0-1 fenceSpec 无结构定义

- **Where**：方案一 §3.1.1、主文档 §9
- **Issue**：仅写「快照上界」，未定义 per-old-bucket 的 `snapshotId`、`logEndOffset`、fence 时刻 leader epoch 等；Buckload 与 VERIFY 无法编码。
- **Impact**：读路径可能丢行或重复行；不同副本 fence 不一致。
- **Fix**：在方案一增加 **fenceSpec 逻辑 schema**（见方案一 §3.1.5 补丁）。

#### P0-2 冷加载后 Log offset 与 KV 一致性（I3）

- **Where**：方案一 §3.1.4、不变量 I3
- **Issue**：Bootstrap Log 从 offset=0 安装，但未说明 HW/LEO、与 KV snapshot 包含的行集如何对齐；delete/tombstone 是否在冷 KV 中体现。
- **Impact**：切换后 Upsert/CDC 语义错误。
- **Fix**：明确「冷 KV = fence 点 PK 最终态」「Bootstrap Log 仅含 layout_switch + 可选 fence 标记，不含全量历史」；LOAD 后 TS 初始化规则写清。

#### P0-3 VERIFY 行数守恒未考虑删除标记

- **Where**：方案一 §4.2
- **Issue**：PK 表存在 delete/update；Σ rowCount 若按「行数」而非「有效 PK 数」可能误判。
- **Impact**：VERIFY 误通过或误失败。
- **Fix**：VERIFY 改为 **有效 PK 计数**（含 tombstone 规则），与 merge engine 一致。

### P1 — 实现者仍会猜

#### P1-1 缩桶（N_new < N_old）无算法

- **Where**：方案一（仅提「可缩桶」）
- **Issue**：多旧桶合并到新桶时，Buckload `keyBy(newBucketId)` 如何合并、manifest 是否一旧多新映射未写。
- **Fix**：增加 §3.3 缩桶：旧桶集合 → 新桶映射、单 manifest 多源 fenceSpec 合并规则。

#### P1-2 notifyLoadBuckload 幂等与重试

- **Where**：主文档 §14.2 待决 #2
- **Issue**：TS 重启、重复 notify、LOAD 一半失败时的幂等键未定义。
- **Fix**：RPC 表增加 `jobId + newBucketId + attemptId` 幂等；LOAD 状态机子状态。

#### P1-3 Tiering 与 Rescale 互斥的操作协议

- **Where**：方案一 §3.1（一句「暂停 Tiering」）
- **Issue**：谁 pause、in-flight tier 任务如何处理、resume 条件未写。
- **Fix**：RescaleJob PREPARING 阶段向 Tiering coordinator 发 fence；COMPLETED 后 resume。

#### P1-4 layout_switch proto 仍为待决

- **Where**：主文档 §14.2、方案一 §8
- **Issue**：M2 验收依赖 CDC，但无字段草案。
- **Fix**：增加附录或 §8.1 逻辑字段表（oldEpoch, newEpoch, newBucketCount, partitionId）。

#### P1-5 方案一缺端到端时序图

- **Where**：方案一全文
- **Issue**：主文档精简后，方案一应有完整 sequenceDiagram（评审清单未满足）。
- **Fix**：已补 §3.1.6 时序图（见补丁）。

### P2 — 质量与可读性

#### P2-1 行业调研过薄

- **Where**：主文档 §13
- **Issue**：无竞品对比表；Challenger 早期指出的漏洞未系统归档。
- **Fix**：见本文 §4 竞品矩阵；后续可拆 `scheme-00-industry-survey.md`。

#### P2-2 方案三/四缺迁移时序图

- **Issue**：vnode 分裂、索引分配的原子步骤未图示。
- **Fix**：M3+ 预研前补充。

#### P2-3 方案五 §4.3 并行度表述

- **Issue**：`parallelism >= max(bucketCount)` 易被理解为「布局约束」；实为 Flink Sink **通道分配**运维建议，与 Fluss 物化布局无关。
- **Fix**：改写为「业务 Sink 通道数建议 ≥ 当前写入涉及的最大 per-partition 桶数」。

#### P2-4 Hub 与 Scheme 重复

- **Issue**：RescaleJob 状态机在 hub §9 与 scheme-01 §4 重复。
- **Status**：可接受（hub 摘要 + scheme 细节），但 scheme-01 应自包含更多细节而非仅「详见主文档」。

---

## 4. 竞品矩阵（补主文档 §13）

| 产品 | 机制 | PK 表扩缩 | 湖对齐 | 停写 | Fluss 借鉴 |
|------|------|-----------|--------|------|------------|
| **Paimon** | Fixed: 分区 INSERT OVERWRITE；Dynamic: 单写者索引 | Fixed 离线 overwrite | 原生 | Fixed 需维护 | 方案一+五对齐 Fixed |
| **Doris** | 新分区新 BUCKETS；旧分区不变 | 分区级 | 外表各异 | 旧分区改桶需替换 | 方案五直接对标 |
| **Kafka** | 仅增 partition，**不重 hash** | N/A（无 PK） | — | — | **禁止**模仿 |
| **Flink Key Group** | 固定逻辑分片 + 区间重划 | 状态层非存储 | — | rescale 需 savepoint | 仅 **方案四** 同构参考 |
| **Hudi RFC-42** | vnode 区间分裂 | MOR PK | 自有 | 有界迁移 | 方案四对标 |
| **Iceberg** | PK 表禁止 partition spec 演化 | 需新表 | 强 | — | 支持「不能零迁移」论点 |

---

## 5. 文档厚度（行数 vs 目标）

| 文档 | 当前 | 目标（authoring skill） | 差距 |
|------|------|-------------------------|------|
| Hub | ~463 | 300–450 | 合格 |
| Scheme-01 | ~248 | 350+ | **需加厚** |
| Scheme-02 | ~143 | 150+ | 边界 |
| Scheme-03 | ~167 | 200+ | 略薄 |
| Scheme-04 | ~194 | 200+ | 接近 |
| Scheme-05 | ~204 | 200+ | 合格 |

---

## 6. Remediation 计划（建议顺序）

1. **本轮**：闭合方案一 P0/P1（fenceSpec、I3、VERIFY、时序图、失败模式表）— 见 scheme-01 补丁提交  
2. **下一轮**：`scheme-00-industry-survey.md` 或扩展 §13；layout_switch proto 草案  
3. **M3 前**：方案三/四 PoC 时序与 ADR（`docs/decisions/`）  
4. **持续**：每个 milestone 前运行 `/architecture-design-review`

---

## 7. 评审后 Verdict

| 用途 | 判定 |
|------|------|
| 架构方向评审 | **通过** — 主轨（五+一+Buckload）可继续 |
| M2 开发 kickoff | **有条件通过** — 须先闭合 P0-1～P0-3 |
| 对外发布为 Spec | **不通过** — P1 与调研债仍多 |

---

## 参考资料

- [架构总览](./dynamic-bucket-rescaling-design.md)
- [方案一](./scheme-01-offline-redistribution.md)
- Skill: `.cursor/skills/architecture-design-review`
