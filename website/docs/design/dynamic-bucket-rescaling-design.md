---
title: "Primary Key Table Dynamic Bucket Design"
sidebar_label: Dynamic Bucket Design
sidebar_position: 1
---

# Fluss 主键表动态分桶 — 目标架构设计

> **文档类型**：目标架构与时序设计（不含代码实现细节）  
> **文档状态**：Target Architecture  
> **关联 Roadmap**：[Operational Excellence — Automated cluster rebalancing and bucket rescaling](/roadmap)

---

## 目录

1. [执行摘要](#1-执行摘要)
2. [现状基线与目标能力](#2-现状基线与目标能力)
3. [问题、不变量与验收标准](#3-问题不变量与验收标准)
4. [总体方案](#4-总体方案)
5. [分桶函数与跨层对齐规约](#5-分桶函数与跨层对齐规约)
6. [核心能力设计](#6-核心能力设计)
7. [湖流一体与联合读取](#7-湖流一体与联合读取)
8. [运维决策与作业协同](#8-运维决策与作业协同)
9. [桶数调整任务（RescaleJob）](#9-桶数调整任务rescalejob)
10. [元数据模型](#10-元数据模型)
11. [Flink 连接器改造](#11-flink-连接器改造)
12. [行业参考](#12-行业参考)
13. [风险与待决事项](#13-风险与待决事项)

---

## 1. 执行摘要

### 1.1 问题

Fluss 主键表在建表时通过 `bucket.num` 固定分桶数量，建表后无法变更。用户需要在 **不破坏主键唯一性、Upsert 语义、变更数据捕获（CDC）连续性** 的前提下，按全表或按分区 **增加或减少** 分桶数量，并与湖仓分层、Flink 作业协同。

### 1.2 推荐方案

**默认采用固定哈希分桶模式**，组合三种能力：

1. **新分区采用新桶数** — 修改表的默认桶数，仅影响未来创建的分区（分区表）或触发全表离线迁移（非分区表见 §6.4）。
2. **已有分区离线重分布** — 以 **变更日志（Log）为数据真源**，停写停读目标范围 → 全量迁移 → 原子切换布局 → 可选湖层同步。
3. **湖层协同** — 镜像表与 Fluss 桶布局、分桶函数、偏移量版本对齐；联合读取（Union Read）在各阶段语义明确。

三种分桶模式（固定哈希、动态索引、一致性哈希）在 **建表时选型、长期并存**；同一张表不在生命周期内自动切换模式。

### 1.3 交付里程碑

| 里程碑 | 交付能力 |
|--------|----------|
| **M1** | 分区级桶布局元数据；新分区默认桶数可变更；Flink 按分区解析桶数（布局变更须 Savepoint 协同，见 §11） |
| **M2** | RescaleJob 状态机（含失败回滚）；离线重分布；写入/读取隔离；CDC 布局切换事件；缩桶 |
| **M3** | 湖表按分区桶数对齐；联合读取与 Paimon 协同；开湖/关湖与扩缩桶时序；分层偏移量与 `layoutEpoch` 绑定 |

动态索引分桶、一致性哈希分桶在 M3 之后 **单独立项评估**，不阻塞主路径。

---

## 2. 现状基线与目标能力

### 2.1 当前已实现

| 能力 | 现状 |
|------|------|
| 分桶路由 | 固定 `hash(分桶键) % bucket.num`；开湖后按湖格式切换分桶函数（见 §5） |
| 主键表分桶键 | 未显式指定时自动设为物理主键（排除分区键） |
| `bucket.num` / `bucket.key` | 建表时确定，**ALTER 不可改**（服务端与 Flink Catalog 均禁止） |
| 桶数作用域 | **表级单一** `numBuckets`；各分区桶数相同 |
| 集群 Rebalance | 在桶 **数量不变** 前提下迁移副本，与扩缩桶正交 |
| 动态开湖 | `ALTER SET table.datalake.enabled=true` 时按 **当前** 桶布局创建镜像湖表 |
| Flink Sink 桶数 | SQL/DataStream Sink 在 **作业构建/规划期** 固化 `numBuckets` |
| RescaleJob / layoutEpoch | **未实现** |

### 2.2 目标能力（本文定义）

| 能力 | 目标行为 |
|------|----------|
| 默认桶数变更 | `ALTER` 修改 `defaultBucketCount`；已有分区/非分区表布局不变 |
| 分区级扩缩 | RescaleJob 对指定分区（或非分区表的逻辑分区）离线重分布 |
| 元数据 | `PartitionBucketLayout` + `layoutEpoch` + `rescaleState` |
| 写入/读取隔离 | 目标范围停写 **且** 停读（点查、扫描、Lookup、联合读取，见 §9.4） |
| CDC | 迁移窗口边界发送 `layout_switch` 控制事件；下游可识别布局版本 |
| 湖层 | 已开湖表的 Paimon overwrite 与 Fluss 切换原子协同 |
| 分桶模式 | 建表可选 `bucketMode`（首期仅实现 `FIXED_HASH`） |

### 2.3 与集群 Rebalance 的边界

Rebalance **搬移已有桶的副本**；动态分桶 **改变桶数量并重分布数据**。二者可串行执行，但 **同一分区同一时刻不可并行**（见 §9.6）。

---

## 3. 问题、不变量与验收标准

### 3.1 问题陈述

如何在主键表上允许运行时调整分桶数量（全局或按分区），并协调客户端路由、Coordinator 元数据、TabletServer 存储（Log + KV）、湖仓分层、Flink 作业的一致性？

### 3.2 核心不变量

| 编号 | 不变量 | 验收要点 |
|------|--------|----------|
| **I1** | 主键唯一性 | 任意时刻每个主键最多一个有效行版本 |
| **I2** | 路由确定性 | 给定 `(layoutEpoch, 主键)` 路由到唯一 `bucketId` |
| **I3** | Log-KV 一致 | 迁移完成后，同 TableBucket 的 Log 与 KV 可互相恢复 |
| **I4** | CDC 可解释 | 布局切换前后，changelog 可通过 `layout_switch` 事件与 epoch 关联 |
| **I5** | 联合读取正确 | 湖快照与 Fluss 日志按主键 merge，以日志为准 |
| **I6** | 湖层对齐 | 分层偏移量携带 `layoutEpoch`；跨层 split 按 `(partition, bucketId, epoch)` 配对 |
| **I7** | 分区间隔离 | 分区 A 的扩缩不破坏分区 B 的 I1–I6 |

### 3.3 设计目标优先级

| 优先级 | 目标 |
|--------|------|
| **P0** | 正确性、可运维（API、进度、失败回滚） |
| **P1** | 湖层对齐、CDC 连续性 |
| **P2** | 缩桶、自动化、可选在线模式 |

### 3.4 范围边界

**在范围内**：主键表、固定哈希模式（M1–M3）；分区表与非分区表。

**不在首期范围**：

- 修改 `bucket.key` 列集合
- 日志表（非主键表）动态分桶
- 改变 `max.bucket.num` 全局上限语义
- 动态索引分桶、一致性哈希分桶的实现（仅保留架构占位）

---

## 4. 总体方案

### 4.1 设计原则

1. 主键表不存在「只改元数据、零迁移」的通用解法。
2. 分桶模式在建表时选定；默认路线与可选模式 **并列**，非同表进化。
3. 湖流一体是默认约束面，即使当前未开湖也须考虑后续开湖时序。
4. Flink 并行度与 Fluss 桶数是 **独立维度**；布局变更须 Savepoint 或作业重启协同。
5. 离线迁移以 **Log 为数据真源**；KV 由 Log 重放或校验对齐生成。

### 4.2 默认路线：固定哈希分桶

| 能力 | 作用 | 适用 |
|------|------|------|
| **新分区采用新桶数** | 修改 `defaultBucketCount`，未来分区用新默认值 | 时间分区表、负载随时间变化 |
| **已有分区离线重分布** | Log 驱动全量迁移 + 原子切换 | 热点历史分区必须扩/缩桶 |
| **湖层协同** | 镜像表布局对齐 + 联合读取阶段语义 | 已开湖或计划开湖 |

建表时 `bucketMode = FIXED_HASH`。

### 4.3 可选分桶模式（远期）

| 模式 | 适用 | 首期 |
|------|------|------|
| **动态索引分桶** | 频繁在线扩桶、可接受单写者 | 不实现 |
| **一致性哈希分桶** | 超大表、有界在线迁移 | 不实现 |

二者均须 **新建表** 选型，不能从固定哈希表原地升级。

### 4.4 能力对比（定性，含前提）

| 能力 | 离线重分布 | 新分区新桶数 | 动态索引 | 一致性哈希 |
|------|------------|--------------|----------|------------|
| **前提** | 可接受目标范围停写停读 | 分区表 + 仅新分区需新 N | 单写者、不缩桶 | 高工程复杂度 |
| **正确性** | 最高 | 高（旧分区不变） | 高（索引一致时） | 高（迁移完成时） |
| **联合读取** | 与 Paimon Fixed 对齐最好 | 需 per-partition 枚举 | 须 Fluss 索引主导 | 中等 |
| **在线性** | 低（维护窗） | 高（仅新分区） | 高（扩桶） | 中（有界迁移） |
| **缩桶** | 支持（成本高） | 不支持旧分区 | 不支持 | 支持 |

默认路线 = **新分区新桶数 + 离线重分布 + 湖层协同**。

---

## 5. 分桶函数与跨层对齐规约

### 5.1 为何不能简化为单一 hash 公式

Fluss 按是否绑定湖格式选用不同分桶函数与 Key 编码器：

| 场景 | 分桶函数 | Key 编码 |
|------|----------|----------|
| 纯 Fluss（未绑定湖格式） | `FlussBucketingFunction` | Fluss 编码 |
| 绑定 Paimon | `PaimonBucketingFunction` | `PaimonKeyEncoder` |
| 绑定 Iceberg | `IcebergBucketingFunction` | `IcebergKeyEncoder` |
| 绑定 Hudi | `HudiBucketingFunction` | Hudi 编码 |
| 绑定 Lance | `FlussBucketingFunction` | Fluss 编码 |

**规约 R1**：同一表在 **开湖前后、扩缩前后**，客户端与 TabletServer 必须使用 **与当前 `table.datalake.format` 一致** 的分桶函数与 Key 编码器计算 `bucketId`。

**规约 R2**：离线重分布的目标桶写入、完整性校验抽样、Paimon overwrite，均须使用 **目标 layout 生效时的分桶函数**，而非迁移启动时的旧函数。

**规约 R3**：联合读取按 `(partitionId, bucketId)` 配对 Fluss 与湖 split 时，须保证 **同一主键在两侧映射到相同 bucketId**；扩缩完成后须执行跨层路由抽样验证。

### 5.2 开湖切换点

表从「纯 Fluss」变为「湖格式绑定」时，分桶函数可能切换。若桶数不变，仍须验证开湖前后同一主键的 `bucketId` 是否一致；不一致则 **禁止自动开湖**，要求先离线重分布或新建镜像表。

### 5.3 与 Paimon Fixed Bucket 的对齐

主键表开湖后镜像为 Paimon **Fixed Bucket**（`bucket=N, bucket-key=...`）。Fluss 扩缩桶完成后，Paimon 侧须通过 **分区级 INSERT OVERWRITE** 重组，使湖内布局与 Fluss 新 `bucketCount` 一致。

---

## 6. 核心能力设计

### 6.1 新分区采用新桶数

**语义**：`ALTER TABLE` 修改 `defaultBucketCount`；**已有分区**（及非分区表的当前布局）保持原 `bucketCount`；之后 `CREATE PARTITION` 使用新默认值。

**元数据**：表级 `defaultBucketCount` 可变更；分区级 `bucketCount` 在创建时快照为 `createdWithBucketCount`。

**局限**：同表不同分区桶数不一致；无法自动解救已过热的历史分区。

**Flink 协同**（见 §11）：仅当 **尚无新桶数分区写入** 且 Sink 尚未需要新 N 时，可不停作业；一旦出现新 N 分区写入或 per-partition 桶数差异， **须 Savepoint 重启** 以刷新 Sink shuffle 布局。

### 6.2 已有分区离线重分布

**最小操作单元**：一个 `(tableId, partitionId)`；非分区表视为 `partitionId = null` 的单一逻辑分区（见 §6.4）。

**数据真源**：**Log（WAL changelog）**。迁移流程以 Log 扫描为主路径，按主键序重放 Upsert/Delete 到目标桶；KV 由重放构建或在 VERIFYING 阶段与 Log distinct PK 对账。

**典型时序**：

1. 提交 RescaleJob，校验 `max.bucket.num` 预算、无并发 Rebalance/Tiering
2. PREPARING：创建目标桶、生成新 Assignment
3. FENCING：目标范围 **停写且停读**（见 §9.4）
4. MIGRATING：按源桶并行扫描 Log → 按 **目标分桶函数** 写入目标桶 Log+KV
5. VERIFYING：行数守恒、路由抽样、Log-KV 一致、源桶无残留
6. SWITCHING：原子更新 `PartitionBucketLayout`（`layoutEpoch++`）
7. 发送 CDC **`layout_switch`** 控制事件（含 `oldEpoch`, `newEpoch`, `newBucketCount`）
8. CLEANING：下线旧桶
9. LAKE_SYNCING（条件触发，见 §7.4）：Paimon overwrite
10. COMPLETED：解除隔离

**CDC 语义（I4）**：

- FENCING 至 SWITCHING 之间：目标分区 **不产生** 新的可消费 changelog（或仅产生带 `RESCALE_FENCED` 标记的心跳）
- SWITCHING 瞬间：下游收到 `layout_switch`，据此重置 per-bucket offset 或切换订阅布局
- SWITCHING 之后：新写入按新 `bucketId` 路由；下游按新 epoch 消费

**读写行为**：FENCING 后目标范围 **拒绝 Produce、Lookup、Scan、联合读取**；其他分区不受影响。

### 6.3 双读过渡（不采用）

迁移期维护旧/新两套路由虽可消除停读，但读放大、CDC 重复事件、联合读取与 Paimon 对齐成本过高。**默认路线不采用**；仅在纯 Fluss、短窗口、无 CDC 的极端场景经审批可选。

### 6.4 非分区主键表

非分区表在创建时即生成全表 `TableAssignment`，无「新分区新桶数」策略。

| 需求 | 路径 |
|------|------|
| 增加/减少全表桶数 | 对 `partitionId=null` 提交 RescaleJob（离线重分布） |
| 仅改默认桶数 | **对非分区表无效**（无未来分区）；须走 RescaleJob |

`max.bucket.num` 校验：非分区表为 `bucketCount`；分区表为 `sum(各分区 bucketCount)`，扩缩前须预检不超限。

### 6.5 可选模式（架构占位）

**动态索引分桶**：持久化 `主键 → bucketId`；单写者；不缩桶；湖层须 Fluss 索引主导 + Paimon Fixed maxBuckets。

**一致性哈希分桶**：虚拟节点环分裂/合并；扩缩均有界迁移；Log+KV 同步迁移与 per-key 隔离复杂。**与 Flink Key Group 同构的是「固定逻辑分片 + 可变物理归属」**，不是环上 rehash。

---

## 7. 湖流一体与联合读取

### 7.1 联合读取模型

开湖表：**热层（Fluss Log/KV）+ 冷层（湖格式快照）**。主键表每个 `(partition, bucketId, layoutEpoch)` 对应一个 Hybrid Split，湖快照 + Fluss 日志按主键 sort-merge，**以 Fluss 日志为准**。

### 7.2 主键表与湖分桶模式

主键表 **强制** 哈希分桶键，开湖后镜像为 **Fixed Bucket**。Paimon Dynamic/Postpone 仅适用于 **日志表或无 bucket.key 表**，不纳入主键表扩缩决策。

主键表 **按分区不同桶数**：Fluss 侧 per-partition `bucketCount` + 分区级离线重分布；Paimon 侧 **Fixed + 分区级 overwrite**（不用 Dynamic/Postpone 作为主路径）。

### 7.3 动态开湖、关湖与扩缩时序

| 操作 | 行为 |
|------|------|
| `SET table.datalake.enabled=true` | 按 **当时** Fluss `bucketCount` / `bucketKeys` 创建镜像湖表 |
| `RESET table.datalake.enabled` | 停止分层；历史湖数据保留；联合读取停止拼接 |
| `table.datalake.format` 预置 | 可先于 enabled 设置，开湖时仍按当时布局建镜像 |

**推荐**：**先完成 Fluss 扩缩，再开湖** — 镜像表一次性按新布局创建，跳过 LAKE_SYNCING。

**先开湖后扩缩**：须离线重分布 + Paimon 分区 overwrite（LAKE_SYNCING）。

**关湖后扩缩再开湖**：

- 若镜像湖表 **仍存在且布局与 Fluss 一致** → 恢复 tiering，无需 overwrite
- 若镜像 **存在但 bucket 不一致** → **拒绝自动开湖**，要求运维执行 Paimon overwrite 或删除镜像后重建
- 若镜像 **不存在** → 按当前 Fluss 布局 `createTable`

### 7.4 LAKE_SYNCING 触发条件

| 条件 | 是否进入 LAKE_SYNCING |
|------|----------------------|
| 扩缩完成时 **未开湖** | 否 |
| 扩缩完成时 **已开湖**，且湖上该分区 **无历史 tier 数据** | 否（空分区或新分区） |
| 扩缩完成时 **已开湖**，且湖上该分区 **有旧 layout 数据** | **是** — 等待 Paimon overwrite 完成 |

### 7.5 迁移各阶段联合读取行为

| 阶段 | 联合读取 |
|------|----------|
| STABLE | 正常 hybrid merge |
| FENCED / MIGRATING / VERIFYING / SWITCHING | **拒绝**（返回 `PARTITION_RESCALING`） |
| LAKE_SYNCING | 使用该分区 **上一稳定 epoch** 的湖快照 + 当前 Fluss tail（tail 在 FENCED 后不再增长） |
| COMPLETED | 新 layout + 新 `layoutEpoch` 对应的 readable snapshot |

### 7.6 分层偏移量与 layoutEpoch

每个 `(partitionId, bucketId)` 的分层偏移量记录：

```
BucketOffsetEntry:
  partitionId, bucketId, layoutEpoch, logOffset, bucketCountAtTier
```

**规则**：

- 扩缩切换后，新 epoch 的 Fluss log tail 从 **offset=0** 起算，并发送 `layout_switch`
- 湖上 **旧 epoch** 的 tier 数据保留；联合读取仅配对 **相同 layoutEpoch** 的湖快照与 Fluss tail
- 旧 epoch 湖数据不参与新 epoch 的 merge；历史查询须指定 epoch 或通过时间Travel（远期）
- 若需物理清理旧 layout 湖数据，须单独触发 Paimon overwrite（运维操作，非自动）

### 7.7 多湖格式说明

| 格式 | 主键表扩缩 | 联合读取 |
|------|------------|----------|
| **Paimon** | Fixed rescale + overwrite；**M3 完整支持** | 主路径 |
| **Iceberg** | PK 表禁止 partition spec 演化；扩缩须 Fluss 主导 + 湖侧重写 | M3 起评估 |
| **Hudi** | 分区级 bucket replace；与离线重分布类似 | M3 起评估 |
| **Lance** | 分桶函数与 Fluss 相同；布局变更流程待产品化 | M3 起评估 |

首期工程实现以 **Paimon** 为湖层对齐的验收基准；其他格式须在本设计原则下单独补充 runbook。

---

## 8. 运维决策与作业协同

### 8.1 决策维度

1. **表是否分区** → 非分区表只能离线重分布
2. **目标范围** → 仅新分区 vs 已有分区
3. **湖集成状态** → 从未开湖 / 计划开湖 / 已开湖 / 曾开湖已关
4. **操作顺序** → 扩缩与开湖先后
5. **维护窗** → 目标范围停写停读是否可接受

### 8.2 决策流程

```mermaid
flowchart TB
    START["桶数调整需求"] --> NP{"非分区表?"}
    NP -->|是| OFFLINE_NP["全表离线重分布"]
    NP -->|否| D1{"仅新分区用新桶数?"}
    D1 -->|是| NEW_DEFAULT["修改 defaultBucketCount"]
    D1 -->|否| D2{"湖集成状态"}

    D2 --> LAKE_NOW["已开湖"]
    D2 --> LAKE_NEVER["从未开湖"]
    D2 --> LAKE_SOON["未开湖，计划开湖"]
    D2 --> LAKE_OFF["曾开湖已关"]

    LAKE_NOW --> OFFLINE_LAKE["离线重分布 + LAKE_SYNCING"]
    LAKE_NEVER --> OFFLINE_OR_NEW["离线重分布 或 新分区新桶数"]
    LAKE_SOON --> OFFLINE_THEN["离线重分布/新默认 → 再开湖"]
    LAKE_OFF --> OFFLINE_OR_NEW2["离线重分布/新默认；再开湖时校验镜像"]
```

### 8.3 Flink 作业协同 Runbook

#### 已有分区离线重分布（须维护窗）

| 步骤 | 操作 |
|------|------|
| 1 | `STOP WITH SAVEPOINT`（Sink + 消费该表的 Source/Lookup） |
| 2 | `CALL sys.rescale_buckets(table, partition, targetBuckets)` |
| 3 | 轮询至 `COMPLETED`（含 LAKE_SYNCING） |
| 4 | 从 savepoint 恢复；**建议** `parallelism ≥ 该分区新桶数`，非硬性要求（见 §11.4） |

#### 仅修改 defaultBucketCount（分区表，尚无新 N 分区）

| 步骤 | 操作 |
|------|------|
| 1 | `ALTER TABLE SET ('bucket.num' = '新默认值')`（目标 API） |
| 2 | 在 **第一个新 N 分区创建或写入前**，对 Sink 作业执行 Savepoint 重启以刷新桶布局 |
| 3 | Source/Lookup 作业建议在新区写入前重启，避免枚举 split 遗漏 |

#### 布局变更后的 CDC 下游

下游须处理 `layout_switch` 事件：重置 state 中 per-bucket 进度，或切换至新 epoch 的 topic/split 映射。

---

## 9. 桶数调整任务（RescaleJob）

### 9.1 设计原则

- 与 Bucket 状态机、RebalanceManager **正交**
- 一个 `(tableId, partitionId)` 同时最多一个活跃 Job
- Coordinator 单线程事件循环驱动；Job 状态持久化（当前 ZK，ZK 移除后迁移至 KV 元数据，待决）

### 9.2 任务状态

| 状态 | 含义 |
|------|------|
| `SUBMITTED` | 已提交 |
| `VALIDATING` | 前置检查（桶数预算、互斥锁、湖状态） |
| `PREPARING` | 创建目标桶、Assignment |
| `FENCING` | 广播隔离，等待 in-flight 完成 |
| `MIGRATING` | Log 驱动迁移 |
| `VERIFYING` | 完整性校验 |
| `SWITCHING` | 原子切换 layout |
| `CLEANING` | 下线旧桶 |
| `LAKE_SYNCING` | 等待湖层 overwrite（条件触发） |
| `ROLLING_BACK` | 失败或取消后的补偿 |
| `COMPLETED` / `FAILED` / `CANCELED` | 终态 |

```mermaid
stateDiagram-v2
    [*] --> SUBMITTED
    SUBMITTED --> VALIDATING
    VALIDATING --> PREPARING
    VALIDATING --> FAILED
    PREPARING --> FENCING
    FENCING --> MIGRATING
    MIGRATING --> VERIFYING
    VERIFYING --> SWITCHING
    SWITCHING --> CLEANING
    CLEANING --> LAKE_SYNCING
    CLEANING --> COMPLETED
    LAKE_SYNCING --> COMPLETED
    MIGRATING --> ROLLING_BACK
    VERIFYING --> ROLLING_BACK
    SWITCHING --> ROLLING_BACK
    ROLLING_BACK --> FAILED
    MIGRATING --> ROLLING_BACK : cancel
    SUBMITTED --> CANCELED : cancel
    VALIDATING --> CANCELED : cancel
    PREPARING --> CANCELED : cancel
    FAILED --> SUBMITTED : retry
```

### 9.3 写入与读取隔离（Fence）

1. **软通知**：`RescaleNotice` → 客户端 flush
2. **硬隔离**：目标分区 Produce / Lookup / Scan / 联合读取 返回 `PARTITION_RESCALING`

| RPC / 操作 | 隔离期间（目标分区） |
|------------|----------------------|
| Produce | 拒绝 |
| Lookup | 拒绝 |
| Scan | 拒绝 |
| 联合读取 | 拒绝 |
| CreatePartition（其他分区） | 允许 |
| 其他分区读写 | 允许 |

### 9.4 迁移子任务

每源桶：`PENDING → SCANNING_LOG → WRITING → CHECKPOINTING → DONE`。断点：`lastScannedOffset`、`lastPk` 持久化。

### 9.5 完整性校验

1. Log 重放行数 = 源桶 Log distinct PK 操作数（按 merge 语义）
2. 目标桶 KV distinct PK = 目标桶 Log distinct PK
3. 随机主键：按目标分桶函数计算的 `bucketId` 与存储位置一致
4. 源桶 Log+KV 残留为 0

### 9.6 失败、取消与回滚

| 失败点 | 补偿动作 |
|--------|----------|
| MIGRATING 失败/取消 | ROLLING_BACK：删除目标桶已写入数据；恢复 WriteFence 前布局；**不递增** layoutEpoch |
| VERIFYING 失败 | 同上 |
| SWITCHING 失败 | ZK 事务未提交则自动回滚；已提交则人工介入（禁止自动重试 SWITCHING） |
| CLEANING 失败 | 标记 FAILED，旧桶人工清理；layout 已切换，**不可自动 ROLLING_BACK** |
| LAKE_SYNCING 失败 | Fluss 侧 COMPLETED 可回滚为 FAILED；湖层 overwrite 须运维重试 |

取消：仅 `SUBMITTED` / `VALIDATING` / `PREPARING` 可无损 `CANCELED`；进入 `MIGRATING` 后取消走 ROLLING_BACK。

### 9.7 与其他操作的互斥

| 操作 A | 操作 B | 结果 |
|--------|--------|------|
| Rescale(P) | Rescale(P) | 第二个拒绝 |
| Rescale(P) | Rescale(Q) | 允许并行 |
| Rescale(P) | Rebalance(bucket ∈ P) | 互斥 |
| Rescale(P) | Tiering(P) | Tiering 暂停 |
| Rescale(P) | DropPartition(P) | 拒绝 |
| Rescale(P) | DropTable | 拒绝 |
| Rescale(P) | AlterOpenLake（同表） | 拒绝，须等 Job 终态 |

### 9.8 Admin API

| 接口 | 语义 |
|------|------|
| `rescaleBuckets(table, partition?, targetBuckets)` | 提交 Job |
| `getRescaleProgress(jobId)` | 查询进度 |
| `cancelRescaleJob(jobId)` | 取消 |
| `retryRescaleJob(jobId)` | 从 FAILED 重试（新 JobId 或复用，实现待定） |

---

## 10. 元数据模型

### 10.1 表级（TableRegistration）

| 字段 | 说明 |
|------|------|
| `defaultBucketCount` | 新分区默认桶数；兼容原 `bucketCount` |
| `bucketMode` | `FIXED_HASH`（首期唯一实现值） |
| `maxBucketCount` | 可选模式上限（首期保留字段） |
| `bucketLayoutVersion` | 协议版本 |

```
resolveBucketCount(table, partition):
  if partition == null: return table.defaultBucketCount  // 非分区表
  if partition.bucketCount != null: return partition.bucketCount
  return table.defaultBucketCount
```

### 10.2 分区级（PartitionRegistration）

| 字段 | 说明 |
|------|------|
| `bucketCount` | null 则继承 default |
| `layoutEpoch` | 扩缩完成时 +1 |
| `rescaleState` | STABLE / FENCED / MIGRATING / LAKE_SYNCING |
| `activeJobId` | 当前 RescaleJob |
| `createdWithBucketCount` | 创建时快照 |

### 10.3 PartitionInfo RPC

须返回 `bucketCount`、`layoutEpoch`、`rescaleState`。分区表路由 **不得** 仅用 `TableInfo.numBuckets`。

### 10.4 ZK 路径

| 路径 | 内容 |
|------|------|
| `/fluss/tables/{id}/registration` | TableRegistration |
| `/fluss/tables/{id}/partitions/{pid}/registration` | PartitionRegistration |
| `/fluss/tables/{id}/rescale-jobs/{jobId}` | RescaleJob |
| `/fluss/tables/{id}/write-fences/{pid}` | WriteFence |
| `/fluss/tables/{id}/layout-history/{pid}` | LayoutHistory |

---

## 11. Flink 连接器改造

### 11.1 问题

Sink 在规划期固化 `numBuckets`；Source 虽可在 enumerator 重启时刷新，但 per-partition 桶数、layoutEpoch 变更无统一抽象。

### 11.2 BucketLayoutProvider

```
getDefaultBucketCount()
getPartitionBucketCount(partitionName)   // 非分区表用 null
getPartitionLayoutEpoch(partitionName)
getPartitionRescaleState(partitionName)
refresh() / subscribeLayoutChanges()
```

### 11.3 改造项

**Sink**：`ChannelComputer` / `HashBucketAssigner` 接受 Provider；捕获 `PARTITION_RESCALING` 后 refresh + 退避；**布局变更后须 Savepoint 重启**（M1 不接受纯 runtime 热更新为默认路径）。

**Source / 联合读取**：`LakeSplitGenerator` per-partition 枚举；Enumerator 订阅 layout 变更；`LAKE_SYNCING` 降级策略。

**Catalog**：暴露 `ALTER` 改默认桶数、`CALL sys.rescale_buckets`。

**RecoveryOffsetManager / Lookup**：per-partition 桶枚举；FENCED 分区拒绝服务。

### 11.4 并行度与桶数

| 关系 | 说明 |
|------|------|
| `parallelism >= 新桶数` | **建议**，便于 1:1 subtask-bucket，非 API 硬约束 |
| `parallelism < 新桶数` | 允许多桶映射同一 subtask；须验证 `ChannelComputer` 与 weighted assigner |
| `parallelism > 新桶数` | 常见；多 subtask 共享桶 |

### 11.5 交付分期

| 阶段 | 内容 | 前置 |
|------|------|------|
| **基础** | Provider + PartitionInfo + Sink/Source 按分区 N | 服务端 Schema + RPC |
| **增强** | 联合读取、Recovery、layout_switch CDC | 基础 |
| **完善** | 指标、多格式湖 runbook | 增强 |

**上线顺序**：服务端元数据与 RPC **先于** Connector 发布。

---

## 12. 行业参考

业界解决同一矛盾：**分片数变化** vs **主键路由稳定性**。

| 系统 | 做法 | 对 Fluss 的启示 |
|------|------|-----------------|
| **Paimon Fixed** | ALTER + INSERT OVERWRITE | 离线重分布主路径 |
| **Paimon Dynamic** | Key→Bucket 索引，单写者 | 动态索引占位 |
| **Iceberg** | PK 表禁止 spec 演化 | 元数据演化不适用 PK |
| **Hudi CH** | 环分裂，局部迁移 | 一致性哈希占位 |
| **Kafka** | 增分区不迁数据 | **禁止** rehash 不迁数据 |
| **Doris** | 旧分区不变，新分区新 N | 新分区新桶数 |
| **StarRocks** | 后台 tablet 搬运 | 引擎内在线迁移参考 |
| **Flink Key Group** | 固定逻辑分片 + 可变 subtask 归属 | Connector Savepoint 协同；**非**存储层 CH |

Flink Key Group 要点：`hash(key) % maxParallelism` 在作业生命周期内 **固定**；rescaling 搬迁的是 Key Group 状态块，不是改变 key→分片规则。Fluss 一致性哈希（若实现）与 Key Group **同构的是两层映射**，但须迁移持久化 Log+KV。

---

## 13. 风险与待决事项

### 13.1 技术风险

| 风险 | 缓解 |
|------|------|
| Log 重放与 KV 不一致 | VERIFYING 对账；以 Log 为准修复 KV |
| 大分区迁移过长 | per-bucket 并行；限速；可观测进度 |
| 湖层 overwrite 失败 | LAKE_SYNCING 阻塞 COMPLETED；运维重试 |
| Coordinator 故障 | Job 状态持久化；恢复后续跑 |
| 与 Rebalance 死锁 | 全局互斥锁 + 优先级 |

### 13.2 待决事项

1. `layout_switch` CDC 事件的精确 schema 与 Kafka/Flink CDC 封装格式
2. 桶编号是否允许非连续（扩缩后）
3. ZK 移除后 RescaleJob / Fence 的存储后端
4. Iceberg/Hudi/Lance 的 LAKE_SYNCING 自动化程度（M3 范围）
5. 动态索引 / 一致性哈希是否纳入产品路线图及触发条件

### 13.3 结论

Fluss 主键表固定分桶是并行度与长期运维的瓶颈；Bucket 状态机、Assignment、Rebalance 提供了扩展底座。

**推荐路径**：固定哈希模式下，**新分区新桶数 + 已有分区离线重分布（Log 驱动）+ 湖层协同**，按 M1→M2→M3 交付。动态索引与一致性哈希为 **并列可选模式**，须新建表选型，不在首期实现范围。

联合读取、分桶函数对齐、CDC 布局切换事件是正确性的核心约束；Flink 并行度与桶数独立，布局变更须 Savepoint 协同。

---

## 参考资料

- [Fluss Bucketing](/table-design/data-distribution/bucketing.md)
- [Fluss Primary Key Table](/table-design/table-types/pk-table.md)
- [Fluss Rebalance](/maintenance/operations/rebalance.md)
- [Fluss Architecture](/concepts/architecture.md)
- [Paimon Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
- [Paimon Rescale Bucket](https://paimon.apache.org/docs/master/maintenance/rescale-bucket/)
- [Flink Rescalable State](https://flink.apache.org/2017/07/04/a-deep-dive-into-rescalable-state-in-apache-flink/)
- [Hudi RFC-42 Consistent Hashing](https://github.com/apache/hudi/blob/master/rfc/rfc-42/rfc-42.md)
