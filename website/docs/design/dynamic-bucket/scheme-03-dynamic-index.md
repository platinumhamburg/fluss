---
title: "Scheme 3 — Dynamic Index Bucketing"
sidebar_label: "Scheme 3 Dynamic Index"
sidebar_position: 3
---

# 方案三：动态索引分桶

> **父文档**：[主键表动态分桶 — 架构总览](../dynamic-bucket-rescaling-design.md)  
> **路由范式**：`bucketMode = DYNAMIC_INDEX`  
> **对标**：Paimon Dynamic Bucket（`bucket = -1`）  
> **交付**：M3 之后单独立项；**不阻塞**主轨 M1–M3

---

## 1. 方案定义

### 1.1 核心思想

不依赖 `hash(key) % N` 决定桶归属，而维护持久化映射：

```text
主键 ──► bucketId（由索引表 / 索引服务决定）
```

- **已存在主键**：永驻原 `bucketId`（扩桶不搬数据）
- **新主键**：按负载策略分配至现有或新增桶
- **扩桶**：增加空桶，仅新 key 或 rebalance 策略使用新桶

### 1.2 适用场景

| 适合 | 不适合 |
|------|--------|
| 需 **频繁在线扩桶**、不能接受长维护窗 | 需要 **缩桶** |
| 可接受 **单写者**（每表/分区） | 多 Flink 作业并发写同表 |
| 写入吞吐可换索引维护成本 | 要求与 Paimon Dynamic 双索引 |

### 1.3 建表选型

`bucketMode = DYNAMIC_INDEX` 须在 **建表时** 指定。不能从 [方案一/五](./scheme-01-offline-redistribution.md) 的 `FIXED_HASH` 表原地升级（索引模型与 `% N` 不兼容）。

---

## 2. 路由语义

### 2.1 读路径

1. 查 `PK → bucketId` 索引  
2. 若命中 → 路由至该桶  
3. 若未命中（新 key）→ 分配算法选择桶（如最小负载）→ **原子**写入索引后再写数据  

### 2.2 写路径

**单写者约束**：同表（或同分区）同一时刻仅一个写入者（单个 Flink job 或单个 primary writer），否则可能重复分配 bucket，破坏 I1。

### 2.3 扩桶

```text
ALTER 增加 maxBucketCount / 激活新 bucketId
→ 新 PK 可进新桶
→ 已有 PK 不迁移
```

无需 [方案一](./scheme-01-offline-redistribution.md) 式 Buckload。

### 2.4 缩桶

**不支持**。须新建表 + 导出导入，或接受长期空桶。

---

## 3. 索引存储架构（待决）

| 选项 | 优点 | 缺点 |
|------|------|------|
| **Coordinator 集中索引** | 全局视图简单 | 热点、内存/持久化压力 |
| **IndexTablet 分片** | 可扩展 | 与数据桶分离的一致性 |
| **每桶局部索引** | 本地化 | 跨桶查询与分配复杂 |

M3+ 预研须先定 **索引分片与 PK 范围** 是否与 bucket 共置。

---

## 4. 元数据

| 字段 | 说明 |
|------|------|
| `bucketMode` | `DYNAMIC_INDEX` |
| `maxBucketCount` | 上限 |
| `activeBucketCount` | 当前逻辑桶数（可 ≤ max） |
| 索引 | `pk → bucketId`（持久化） |

`layoutEpoch` 在纯扩桶（仅增空桶）时可能 **不变**；若未来支持索引 compaction 重整，另议 epoch 规则。

---

## 5. 湖流一体

### 5.1 原则：Fluss 索引主导

| 层 | 策略 |
|----|------|
| **Fluss** | Dynamic 索引决定 `bucketId` |
| **Paimon** | Fixed Bucket，`bucket = maxBucketCount`；tiering 按 Fluss `bucketId` 写入 |
| **禁止** | Fluss 与 Paimon 各维护独立 Dynamic 索引 |

### 5.2 Union Read

按 Fluss `bucketId` 枚举 split；Paimon 侧读对应 fixed bucket。扩桶后须保证 tiering 写入的 `bucketId` 与索引一致。

### 5.3 与 Paimon Dynamic 差异

Paimon Dynamic 在 PK 表默认 `-1` 且单写者；Fluss 开湖 PK 表走 Fixed。方案三是在 **Fluss 层** 做 Dynamic，**湖层仍 Fixed maxBuckets**。

---

## 6. Flink 协同

| 项 | 要求 |
|----|------|
| 写入 | 单作业写表；或显式 primary writer id |
| 扩桶 | 协调器通知索引与 BucketLayoutProvider refresh |
| 并行度 | 与 `activeBucketCount` 独立；索引 lookup 可能成为瓶颈 |

---

## 7. CDC

扩桶不产生 `layout_switch` 式全量 epoch 跳变；须在 changelog 中可区分 **新桶激活** 事件，或依赖消费位点按桶独立推进。

---

## 8. 优缺点

| 优点 | 缺点 |
|------|------|
| 在线扩桶、无全量迁移 | 索引存储与维护成本 |
| 无 Buckload 维护窗 | 单写者、缩桶不支持 |
| 顺序依赖导致长期不均匀 | 湖对齐复杂于 Fixed |

---

## 9. 与方案四对比

| 维度 | 方案三（索引） | [方案四](./scheme-04-consistent-hashing.md)（CH） |
|------|----------------|---------------------------------------------------|
| key→逻辑分片 | **查表** | **hash 固定逻辑格** |
| 扩桶搬数据 | 否 | 仅受影响区间 |
| 缩桶 | 否 | 支持（有界） |
| 与 Flink Key Group | 不同 | **结构同构** |

---

## 10. 交付与验收

| 阶段 | 内容 |
|------|------|
| 预研 | 索引位置、单写者 API、Paimon 对齐 PoC |
| 实现 | 新表 `DYNAMIC_INDEX` DDL；非迁移工具 |

验收：在线扩桶不停写；索引与 KV 一致；tiering bucketId 对齐；单写者违反时明确失败。

---

## 11. 参考资料

- [Paimon Data Distribution — Dynamic Bucket](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
