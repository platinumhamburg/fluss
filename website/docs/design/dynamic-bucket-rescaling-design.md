---
title: "Primary Key Table Dynamic Bucket Design"
sidebar_label: Dynamic Bucket Design
sidebar_position: 1
---

# Fluss 主键表动态分桶 — 架构总览

> **文档类型**：目标架构与时序设计（总览）  
> **文档状态**：Architecture Specification  
> **关联 Roadmap**：[Operational Excellence — Automated cluster rebalancing and bucket rescaling](/roadmap)

各方案的 **完整规格** 见独立子文档（[§5 五方案索引](#5-五方案索引与选型)）。**Challenger 评审报告**见 [DESIGN-REVIEW](./dynamic-bucket/DESIGN-REVIEW.md)。本文保留跨方案不变量、总体架构、湖流一体约束与交付里程碑；**Buckload、RescaleJob 状态机、单方案读写语义** 等细节下沉至对应子文档。

---

## 目录

1. [执行摘要](#1-执行摘要)
2. [现状与目标](#2-现状与目标)
3. [问题、不变量与范围](#3-问题不变量与范围)
4. [总体架构](#4-总体架构)
5. [五方案索引与选型](#5-五方案索引与选型)
6. [默认主轨](#6-默认主轨)
7. [分桶函数与湖层对齐](#7-分桶函数与湖层对齐)
8. [湖流一体与联合读取](#8-湖流一体与联合读取)
9. [RescaleJob 控制面](#9-rescalejob-控制面)
10. [元数据模型](#10-元数据模型)
11. [Flink 连接器](#11-flink-连接器)
12. [运维指南](#12-运维指南)
13. [行业参考](#13-行业参考)
14. [风险与待决事项](#14-风险与待决事项)

---

## 1. 执行摘要

### 1.1 问题

Fluss 主键表在建表时通过 `bucket.num` 固定分桶数量，建表后无法变更。用户需要在 **不破坏主键唯一性、Upsert 语义、CDC 连续性** 的前提下，按全表或按分区调整分桶数量，并与湖仓分层、Flink 业务作业协同。

### 1.2 方案空间

主键表动态分桶存在 **五种正交或互补的技术路线**（详见 [§5](#5-五方案索引与选型)）。**产品线默认主轨**为：

| 能力 | 对应方案 | 说明 |
|------|----------|------|
| 新分区采用新桶数 | [方案五](./dynamic-bucket/scheme-05-progressive-partition-bucket.md) | `ALTER defaultBucketCount`；仅未来分区 |
| 已有分区离线重分布 | [方案一](./dynamic-bucket/scheme-01-offline-redistribution.md) | FENCE → 重分布 → SWITCH；生产数据面为 **Buckload** |
| 湖层协同 | 跨方案约束 | Paimon 镜像布局对齐；联合读取各阶段语义见 [§8](#8-湖流一体与联合读取) |

[方案二](./dynamic-bucket/scheme-02-dual-read-transition.md)（双读过渡）**不推荐**；[方案三](./dynamic-bucket/scheme-03-dynamic-index.md)（动态索引）、[方案四](./dynamic-bucket/scheme-04-consistent-hashing.md)（一致性哈希）为 **建表时另选 `bucketMode`**，M3 之后单独立项，不阻塞主轨。

三种分桶模式（`FIXED_HASH`、`DYNAMIC_INDEX`、`CONSISTENT_HASH`）**建表选定、长期并存**；同一张表不在生命周期内切换模式。

### 1.3 交付里程碑

| 里程碑 | 内容 |
|--------|------|
| **M1** | 分区级桶布局元数据；`defaultBucketCount` 可变更；Flink 按分区解析桶数（[方案五](./dynamic-bucket/scheme-05-progressive-partition-bucket.md)） |
| **M2** | RescaleJob 控制面；`fluss-flink-buckload`；Buckload RPC；冷包 + 冷加载；缩桶（[方案一](./dynamic-bucket/scheme-01-offline-redistribution.md)） |
| **M3** | 湖表 per-partition 对齐；联合读取；开湖/关湖时序；`layoutEpoch` 与 tier offset 绑定 |

---

## 2. 现状与目标

### 2.1 当前能力

| 项 | 现状 |
|----|------|
| 分桶 | `hash(分桶键) % bucket.num`；开湖后切换为湖格式分桶函数 |
| 主键表分桶键 | 默认物理主键（排除分区键），不可为空 |
| `bucket.num` | 建表固定，ALTER 不可改 |
| 桶数粒度 | 表级单一 `numBuckets` |
| Rebalance | 仅迁移已有桶副本，不改变桶数 |
| Remote storage | 主键表 KV snapshot + remote log segment 已用于容灾 |
| Tiering | 外置 Flink 读 Fluss、写湖 |
| 动态分桶 | **未实现** |

### 2.2 目标能力

| 项 | 目标 |
|----|------|
| 默认桶数 | `ALTER defaultBucketCount`；已有 layout 不变 |
| 分区扩缩 | RescaleJob 编排；数据面默认 Buckload |
| 元数据 | `layoutEpoch`、`rescaleState`、per-partition `bucketCount` |
| 隔离 | 目标范围 FENCE 期间停写停读 |
| CDC | Bootstrap Log 含 `layout_switch`；下游按 epoch 重置 |
| 湖层 | 已开湖时 Paimon overwrite 与 Fluss 冷加载协同 |

### 2.3 与 Rebalance 的边界

Rebalance 改变桶 **副本位置**；动态分桶改变桶 **数量与数据布局**。同一分区同一时刻不可并行执行。

---

## 3. 问题、不变量与范围

### 3.1 核心不变量

| 编号 | 不变量 | 验收 |
|------|--------|------|
| **I1** | 主键唯一性 | 任意时刻每主键最多一行有效版本 |
| **I2** | 路由确定性 | `(layoutEpoch, 主键)` → 唯一 `bucketId` |
| **I3** | Log-KV 一致 | 切换后同桶 Log 与 KV 可互相恢复 |
| **I4** | CDC 可解释 | `layout_switch` 关联 old/new epoch |
| **I5** | 联合读取正确 | 湖快照 + Fluss log merge，以 log 为准 |
| **I6** | 湖层对齐 | tier offset 携带 `layoutEpoch` |
| **I7** | 分区间隔离 | 分区 A 扩缩不影响分区 B |

### 3.2 范围

**包含**：主键表、固定哈希主轨、分区表与非分区表、Buckload 生产路径。

**不包含（首期）**：修改 `bucket.key`、日志表动态分桶、[方案三](./dynamic-bucket/scheme-03-dynamic-index.md)/[方案四](./dynamic-bucket/scheme-04-consistent-hashing.md) 实现、改变 `max.bucket.num` 语义。

---

## 4. 总体架构

### 4.1 设计原则

1. 主键表在 `FIXED_HASH` 下不存在「只改元数据、零迁移」的通用解法（[方案一](./dynamic-bucket/scheme-01-offline-redistribution.md)）。
2. 分桶模式建表选定，长期并存，非同表升级。
3. 湖流一体是默认约束，未开湖也须考虑后续开湖时序。
4. Flink **业务作业**并行度与 Fluss 桶数独立；layout 变更须 Savepoint 协同（[§11](#11-flink-连接器)）。
5. **Coordinator 管控制面，数据面默认外置**：Buckload 避免在 TabletServer 上与在线 PutKV 争抢资源。

### 4.2 逻辑分层（默认主轨：方案一 + Buckload）

```text
┌─────────────────────────────────────────────────────────┐
│  运维 / Admin API（rescaleBuckets）                       │
└───────────────────────────┬─────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────┐
│  Coordinator：RescaleJob 状态机（FENCE / VERIFY / SWITCH）│
└───────┬───────────────────────────────┬─────────────────┘
        │ buckloadHeartbeat 等             │ notifyLoadBuckload
        ▼                               ▼
┌───────────────────┐           ┌─────────────────────────┐
│ Buckload Flink Job │           │ TabletServer 冷加载      │
│ （fluss-flink-     │  remote   │ KvSnapshot + LogSegment  │
│  buckload）        │ ────────► │ 安装到新桶 Leader        │
└───────────────────┘           └─────────────────────────┘
        ▲
        │ Fluss Client 读旧 layout（fence 快照）
```

Buckload 全规格见 [方案一 §3](./dynamic-bucket/scheme-01-offline-redistribution.md#3-数据面实现)。

### 4.3 三种分桶模式（建表选型）

| 模式 | 对应方案 | 首期 |
|------|----------|------|
| **FIXED_HASH** | [方案一](./dynamic-bucket/scheme-01-offline-redistribution.md) + [方案五](./dynamic-bucket/scheme-05-progressive-partition-bucket.md) | **默认实现** |
| **DYNAMIC_INDEX** | [方案三](./dynamic-bucket/scheme-03-dynamic-index.md) | 不实现 |
| **CONSISTENT_HASH** | [方案四](./dynamic-bucket/scheme-04-consistent-hashing.md) | 不实现 |

动态索引、一致性哈希须 **新建表** 选型；不能从 `FIXED_HASH` 表原地升级。

---

## 5. 五方案索引与选型

以下五份文档可 **独立阅读**；本文其余章节描述跨方案公共约束。

| 方案 | 文档 | 路由范式 | 核心手段 | 停写？ | 产品立场 |
|------|------|----------|----------|--------|----------|
| **方案一** | [已有分区离线重分布](./dynamic-bucket/scheme-01-offline-redistribution.md) | `FIXED_HASH` | FENCE + 全量重分布；生产用 **Buckload** | 是（分区级） | **主轨执行层**（M2） |
| **方案二** | [双读过渡](./dynamic-bucket/scheme-02-dual-read-transition.md) | `FIXED_HASH`（双 layout） | 写新读双 / 后台清扫 | 否（理想） | **不推荐**；无实现计划 |
| **方案三** | [动态索引](./dynamic-bucket/scheme-03-dynamic-index.md) | `DYNAMIC_INDEX` | 主键→桶索引；单写者 | 否 | 远期另选模式（M3+） |
| **方案四** | [一致性哈希](./dynamic-bucket/scheme-04-consistent-hashing.md) | `CONSISTENT_HASH` | vnode 环；有界迁移 | 否（理想） | 远期另选模式（M3+） |
| **方案五** | [新分区新桶数](./dynamic-bucket/scheme-05-progressive-partition-bucket.md) | `FIXED_HASH` | `defaultBucketCount`；仅未来分区 | 否 | **主轨策略层**（M1） |

### 5.1 选型决策树

```text
需要改已有分区的 bucketCount？
├─ 否 → 仅未来分区新 N？
│        ├─ 是 → 方案五（非分区表不适用）
│        └─ 否 → 无需 RescaleJob
└─ 是 → 可接受分区级停写维护窗？
         ├─ 是 → 方案一（默认 Buckload）
         └─ 否 → 方案二（不推荐）/ 新建表选方案三或四
```

### 5.2 方案间关系

- **方案五 + 方案一**：默认组合；五避免动历史分区，一处理必须改旧分区 N 的场景。
- **方案一 vs 方案二**：二试图消除维护窗，但湖流一体与 CDC 语义难以保证；**产品线采用方案一**。
- **方案一 vs 方案三/四**：不同 `bucketMode`，建表时互斥选型；三/四在线扩桶复杂度更高，不阻塞 FIXED_HASH 主轨。

非分区表：无「未来分区」→ 全表改 N **只能**走 [方案一](./dynamic-bucket/scheme-01-offline-redistribution.md)；仅改 `defaultBucketCount` **无效**（见 [方案五 §1.3](./dynamic-bucket/scheme-05-progressive-partition-bucket.md#13-不适用场景)）。

---

## 6. 默认主轨

**策略**：[方案五](./dynamic-bucket/scheme-05-progressive-partition-bucket.md) 让新分区自然采用新默认桶数。  
**执行**：[方案一](./dynamic-bucket/scheme-01-offline-redistribution.md) 对必须调整的历史分区（或非分区全表）执行离线重分布。

| 用户意图 | 路径 |
|----------|------|
| 新分区更密/更疏，旧分区不动 | `ALTER defaultBucketCount`（方案五） |
| 某历史分区必须改 N | `rescaleBuckets(partition=…)`（方案一） |
| 非分区全表改 N | `rescaleBuckets` 全表（方案一） |
| 不停写扩缩 | 不在主轨范围；评估方案二或新建表选三/四 |

方案一数据面 **推荐 Buckload**（外置 Flink + remote 冷包 + TS 冷加载）；集群内 Log 重放为备选，见 [方案一 §3.2](./dynamic-bucket/scheme-01-offline-redistribution.md#32-集群内重放备选非生产默认)。

---

## 7. 分桶函数与湖层对齐

### 7.1 分桶函数矩阵

| 湖格式 | 分桶函数 | Key 编码 |
|--------|----------|----------|
| 无（纯 Fluss） | `FlussBucketingFunction` | Fluss |
| Paimon | `PaimonBucketingFunction` | `PaimonKeyEncoder` |
| Iceberg | `IcebergBucketingFunction` | Iceberg |
| Hudi | `HudiBucketingFunction` | Hudi |
| Lance | `FlussBucketingFunction` | Fluss |

**规约**（各方案重分布与路由均须遵守）：

- **R1**：重 hash 与 TS 路由均使用 **目标 layout 生效时** 的分桶函数与编码器
- **R2**：开湖前后若函数切换，扩缩前须验证同 PK 的 bucketId 一致性
- **R3**：联合读取按 `(partition, bucketId)` 配对时，跨层路由抽样验证

主键表开湖后为 Paimon **Fixed Bucket**；per-partition 不同 N 走 Fixed + 分区 overwrite，不用 Dynamic/Postpone 作为主路径。

---

## 8. 湖流一体与联合读取

### 8.1 模型

开湖表：**Fluss 热层 + Paimon 冷层**；主键表按 `(partition, bucketId, layoutEpoch)` Hybrid Split merge，**以 Fluss log 为准**。

### 8.2 开湖与扩缩时序

| 顺序 | 建议 |
|------|------|
| **先 Fluss 扩缩，后开湖** | 镜像表按新 layout 创建；无 LAKE_SYNCING |
| **先开湖，后扩缩** | Buckload 冷加载 + Paimon partition overwrite |

**关湖后再开湖**：镜像存在且 layout 一致 → 恢复 tiering；不一致 → 拒绝自动开湖，须 overwrite 或重建镜像。

### 8.3 LAKE_SYNCING

| 条件 | 进入 LAKE_SYNCING |
|------|-------------------|
| 扩缩完成时未开湖 | 否 |
| 已开湖且该分区无历史 tier 数据 | 否 |
| 已开湖且湖上有旧 layout 数据 | **是** |

### 8.4 各阶段联合读取（方案一 RescaleJob）

| RescaleJob 阶段 | 联合读取 |
|-----------------|----------|
| STABLE | 正常 |
| FENCE / BUCKLOADING / VERIFYING / LOADING / SWITCHING | **拒绝** |
| LAKE_SYNCING | 上一稳定 epoch 湖快照 + 冻结的 Fluss tail |
| COMPLETED | 新 epoch |

### 8.5 layoutEpoch 与 tier offset

Tier offset 记录 `layoutEpoch`；旧 epoch 湖数据不参与新 epoch merge；扩缩后 Fluss tail 从 offset=0 起。

各方案湖层差异见对应子文档（如 [方案三 §5](./dynamic-bucket/scheme-03-dynamic-index.md#5-湖流一体)、[方案四 §6](./dynamic-bucket/scheme-04-consistent-hashing.md#6-湖流一体)）。

---

## 9. RescaleJob 控制面

RescaleJob 是 [方案一](./dynamic-bucket/scheme-01-offline-redistribution.md) 的 Coordinator 编排单元；**完整状态机、VERIFY、回滚、互斥** 见 [方案一 §4](./dynamic-bucket/scheme-01-offline-redistribution.md#4-控制面rescalejob)。

### 9.1 状态概要（Buckload 路径）

| 状态 | 含义 |
|------|------|
| `SUBMITTED` / `VALIDATING` / `PREPARING` | 校验与创建新桶 |
| `FENCING` | 停写停读；生成 fenceSpec |
| `BUCKLOADING` | Buckload Flink 写 remote 冷包 |
| `VERIFYING` | 校验 manifest |
| `LOADING` | Coordinator 推送 TS 冷加载 |
| `SWITCHING` | `layoutEpoch++` |
| `CLEANING` | 下线旧桶；清理 orphan |
| `LAKE_SYNCING` | Paimon overwrite（条件） |
| `ROLLING_BACK` / `FAILED` / `CANCELED` / `COMPLETED` | 终态 |

```mermaid
stateDiagram-v2
    [*] --> SUBMITTED
    SUBMITTED --> VALIDATING --> PREPARING --> FENCING
    FENCING --> BUCKLOADING --> VERIFYING --> LOADING --> SWITCHING
    SWITCHING --> CLEANING
    CLEANING --> LAKE_SYNCING --> COMPLETED
    CLEANING --> COMPLETED
    BUCKLOADING --> ROLLING_BACK --> FAILED
    LOADING --> ROLLING_BACK
```

**备选路径**：集群内重放时，`BUCKLOADING`+`LOADING` 合并为 `MIGRATING`（[方案一 §3.2](./dynamic-bucket/scheme-01-offline-redistribution.md#32-集群内重放备选非生产默认)）。

### 9.2 Admin API

`rescaleBuckets` · `getRescaleProgress` · `cancelRescaleJob` · `retryRescaleJob`

### 9.3 Buckload 协议（摘要）

| RPC | 方向 | 作用 |
|-----|------|------|
| `rescaleBuckets` | Admin → Coordinator | 创建 RescaleJob |
| `buckloadHeartbeat` | Flink → Coordinator | 注册 worker、领取任务与 fenceSpec |
| `commitBuckloadBundle` | Flink → Coordinator | 登记单桶 manifest |
| `notifyLoadBuckload` | Coordinator → TabletServer | 推送冷加载指令 |
| `reportBuckloadLoadComplete` | TabletServer → Coordinator | 单桶 LOAD 结果 |

Manifest 字段、Remote 路径、冷加载步骤见 [方案一 §3](./dynamic-bucket/scheme-01-offline-redistribution.md#3-数据面实现)。

---

## 10. 元数据模型

### 10.1 表级

| 字段 | 说明 |
|------|------|
| `defaultBucketCount` | 新分区默认桶数（[方案五](./dynamic-bucket/scheme-05-progressive-partition-bucket.md)） |
| `bucketMode` | 首期主轨 `FIXED_HASH`；三/四见子文档 |
| `bucketLayoutVersion` | 协议版本 |

### 10.2 分区级

| 字段 | 说明 |
|------|------|
| `bucketCount` | null → 继承 default |
| `layoutEpoch` | SWITCH 时 +1 |
| `rescaleState` | STABLE / FENCED / BUCKLOADING / LOADING / LAKE_SYNCING |
| `activeJobId` | 当前 RescaleJob |

### 10.3 RescaleJob 扩展

| 字段 | 说明 |
|------|------|
| `fenceSpec` | 快照上界 |
| `implementation` | `BUCKLOAD` / `IN_CLUSTER` |
| `buckloadWorkerId` | Flink 作业实例 |
| `committedBundles` | `newBucketId → manifestUri` |
| `loadReports` | `newBucketId → status` |

### 10.4 PartitionInfo RPC

须返回 `bucketCount`、`layoutEpoch`、`rescaleState`；分区表 **不得**仅用 `TableInfo.numBuckets`。

[方案三](./dynamic-bucket/scheme-03-dynamic-index.md#4-元数据)、[方案四](./dynamic-bucket/scheme-04-consistent-hashing.md#4-元数据) 另有索引环 / vnode 相关字段。

---

## 11. Flink 连接器

### 11.1 BucketLayoutProvider

运行时供 Sink/Source 解析 per-partition 桶数与 epoch；布局变更后业务作业须 Savepoint 重启。

### 11.2 改造要点

| 组件 | 改造 |
|------|------|
| Sink `ChannelComputer` | per-partition 动态 N |
| Source / `LakeSplitGenerator` | per-partition 枚举 |
| Catalog | `rescale_buckets` procedure |
| Lookup / Recovery | FENCED 分区拒绝；per-partition bucket 枚举 |

### 11.3 并行度与桶布局

| 场景 | 关系 |
|------|------|
| **Buckload**（[方案一](./dynamic-bucket/scheme-01-offline-redistribution.md)） | Flink 为 **纯计算引擎**；并行度仅影响吞吐。冷包物化布局由 `hash(key) % N_new` 与 `keyBy(newBucketId)` 决定，与作业并发度无关 |
| **业务 Connector** | Sink 规划期固化 `numBuckets`；layout 变更须 Savepoint 重启（见 [方案一 §7](./dynamic-bucket/scheme-01-offline-redistribution.md#7-flink-协同)） |

### 11.4 交付顺序

服务端 Schema + RPC → Connector → 文档与指标。

各方案 Flink 协同细节：[方案一 §7](./dynamic-bucket/scheme-01-offline-redistribution.md#7-flink-协同)、[方案五 §4](./dynamic-bucket/scheme-05-progressive-partition-bucket.md#4-与-flink-业务作业)。

---

## 12. 运维指南

### 12.1 决策要点

1. 非分区表 → 只能 [方案一](./dynamic-bucket/scheme-01-offline-redistribution.md)  
2. 仅新分区新 N → [方案五](./dynamic-bucket/scheme-05-progressive-partition-bucket.md)  
3. 已有分区扩缩 → RescaleJob（默认 Buckload）  
4. 已开湖 → 预期 LAKE_SYNCING  
5. 计划开湖 → 优先先扩缩后开湖  

完整决策树见 [§5.1](#51-选型决策树) 与 [方案五 §9](./dynamic-bucket/scheme-05-progressive-partition-bucket.md#9-运维决策)。

### 12.2 Buckload 维护 Runbook（方案一）

| 步 | 操作 |
|----|------|
| 1 | 业务作业 `STOP WITH SAVEPOINT` |
| 2 | `CALL sys.rescale_buckets(...)` |
| 3 | 确认 Buckload Flink 作业运行且 `buckloadHeartbeat` 注册 |
| 4 | `getRescaleProgress` → `COMPLETED` |
| 5 | Savepoint 恢复业务作业 |

### 12.3 CDC 下游

处理 Bootstrap Log 中 `layout_switch`；重置 per-bucket 消费进度至新 epoch（[方案一 §8](./dynamic-bucket/scheme-01-offline-redistribution.md#8-cdc-语义)）。

---

## 13. 行业参考

主键表扩缩的核心矛盾：**分片数变化** vs **路由稳定性**。结构化竞品矩阵与评审发现见 **[DESIGN-REVIEW §4](./dynamic-bucket/DESIGN-REVIEW.md#4-竞品矩阵补主文档-13)**。

Fluss 选型结论：离线迁移保正确性（[方案一](./dynamic-bucket/scheme-01-offline-redistribution.md)）；新分区渐进（[方案五](./dynamic-bucket/scheme-05-progressive-partition-bucket.md)）；Kafka 式只增不重 hash **禁止**；Flink Key Group 仅作为 [方案四](./dynamic-bucket/scheme-04-consistent-hashing.md) vnode 路由的 **类比参考**，与 Buckload / 业务 Sink 物化布局无关。

---

## 14. 风险与待决事项

### 14.1 风险

| 风险 | 缓解 |
|------|------|
| 冷包 KV / Log 不一致 | manifest 绑定；VERIFY + LOAD 后校验 |
| Remote 孤儿文件 | manifest-first；attempt TTL |
| LOADING 磁盘峰值 | 限制并发 LOAD；离峰 |
| Buckload 读放大旧桶 | Client 限速；低峰窗口 |
| 无 remote 配置的集群 | 降级集群内重放（非生产推荐） |
| 双读过渡（方案二） | **不采用**；开湖表禁止 |

### 14.2 待决

1. `layout_switch` record 的 proto schema  
2. `notifyLoadBuckload` 投递通道（TabletServerGateway vs 内部队列）  
3. 开湖表 Buckload 与 Paimon overwrite 同 job 还是两阶段（M3）  
4. 集群内重放是否保留为长期备选或仅测试用途  
5. [方案三](./dynamic-bucket/scheme-03-dynamic-index.md) 索引存储与单写者选举细节  

### 14.3 结论

Fluss 主键表动态分桶的默认路径是：**[方案五](./dynamic-bucket/scheme-05-progressive-partition-bucket.md) + [方案一](./dynamic-bucket/scheme-01-offline-redistribution.md)（Buckload）+ 湖层协同**。五份方案子文档承载各路线完整规格；本文定义跨方案不变量与交付节奏。M1→M2→M3 分批交付。

---

## 参考资料

- [方案一：离线重分布](./dynamic-bucket/scheme-01-offline-redistribution.md)
- [方案二：双读过渡](./dynamic-bucket/scheme-02-dual-read-transition.md)
- [方案三：动态索引](./dynamic-bucket/scheme-03-dynamic-index.md)
- [方案四：一致性哈希](./dynamic-bucket/scheme-04-consistent-hashing.md)
- [方案五：新分区新桶数](./dynamic-bucket/scheme-05-progressive-partition-bucket.md)
- [设计评审报告（Challenger）](./dynamic-bucket/DESIGN-REVIEW.md)
- [Fluss Bucketing](/table-design/data-distribution/bucketing.md)
- [Fluss Primary Key Table](/table-design/table-types/pk-table.md)
- [Fluss Remote Storage](/maintenance/tiered-storage/remote-storage.md)
- [Fluss Rebalance](/maintenance/operations/rebalance.md)
- [Paimon Rescale Bucket](https://paimon.apache.org/docs/master/maintenance/rescale-bucket/)
