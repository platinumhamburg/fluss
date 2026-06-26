---
title: "Scheme 5 — Progressive Partition Bucket Count"
sidebar_label: "Scheme 5 Progressive Partition"
sidebar_position: 5
---

# 方案五：新分区采用新桶数（渐进策略）

> **父文档**：[主键表动态分桶 — 架构总览](../dynamic-bucket-rescaling-design.md)  
> **路由范式**：`FIXED_HASH`（仅改变 **未来分区** 的 `bucketCount`）  
> **对标**：Apache Doris「旧分区不变、新分区新 BUCKETS」  
> **交付**：M1

---

## 1. 方案定义

### 1.1 核心思想

```text
ALTER TABLE SET defaultBucketCount = N_new
```

- **已有分区**：`bucketCount` **不变**，仍按创建时快照的 N_old 路由  
- **未来创建的分区**：使用 `N_new` 作为 `bucketCount`  
- **热点旧分区**须改 N：另启 [方案一](./scheme-01-offline-redistribution.md) 对该分区 Buckload  

### 1.2 适用场景

| 适合 | 不适合 |
|------|--------|
| **时间分区表**（日/月分区） | 非分区表（无「未来分区」） |
| 旧分区稀疏、新分区密集 | 必须扩 **当前唯一分区** 的桶数 |
| 希望 **零迁移** 获得新分区新并行度 | 要求全表统一 N |

### 1.3 在默认主轨中的位置

方案五是 **策略层**（何时用新 N），[方案一](./scheme-01-offline-redistribution.md) 是 **执行层**（旧分区如何改 N）。二者 **组合** 为默认主轨，非同阶段替代。

---

## 2. 核心机制

### 2.1 元数据语义

| 层级 | 字段 | ALTER 后行为 |
|------|------|--------------|
| 表 | `defaultBucketCount` | 更新为 N_new |
| 已有分区 | `bucketCount` | **保持**创建时确定的值 |
| 新分区 | `bucketCount` | 创建时设为当前 `defaultBucketCount`（N_new） |
| 审计 | `createdWithBucketCount` | 分区创建时快照 |

```text
resolveBucketCount(table, partition):
  if partition.bucketCount != null: return partition.bucketCount
  return table.defaultBucketCount
```

### 2.2 创建分区

`CREATE PARTITION`（或自动分区）时：

```text
partition.bucketCount = table.defaultBucketCount
partition.createdWithBucketCount = partition.bucketCount
```

当前代码路径（目标改造）：`CoordinatorService` 新分区须读 **defaultBucketCount** 而非固定表级 `bucketCount`。

### 2.3 非分区表

非分区表在建表时即生成全表 Assignment，**无后续分区**。

| 操作 | 效果 |
|------|------|
| 仅 `ALTER defaultBucketCount` | **无效**（无新分区可消费新默认值） |
| 改全表 N | 必须 [方案一](./scheme-01-offline-redistribution.md) RescaleJob |

---

## 3. 路由与读写

### 3.1 同表多 N 共存

同一表不同分区可有不同 `bucketCount`：

- 分区 `p202401`：`N=16`  
- 分区 `p202506`：`N=64`  

客户端路由：**先解析分区 → 再 `hash % partition.bucketCount`**。

### 3.2 扫描 / 联合读取

须 **per-partition** 枚举 `[0, bucketCount)`，禁止仅用 `TableInfo.numBuckets`。

影响组件：`LakeSplitGenerator`、`FlinkSourceEnumerator`、`RecoveryOffsetManager` 等（见 [主文档 §11](../dynamic-bucket-rescaling-design.md#11-flink-连接器)）。

---

## 4. 与 Flink 业务作业

### 4.1 仅改 default、尚无新 N 分区

若 Sink 尚未遇到新 N 分区，且规划期 `numBuckets` 与当前写入分区一致，**可短暂不停作业**。

### 4.2 出现新 N 分区或 per-partition 差异

Sink 在 SQL 规划期固化 `numBuckets`；`ChannelComputer` 须 per-record 解析分区后取 N。

**在第一个新 N 分区写入前**：业务作业 **Savepoint 重启**，刷新 BucketLayoutProvider。

### 4.3 并行度

`parallelism >= max(各活跃分区 bucketCount)` 为运维建议，非 API 硬约束。

---

## 5. 湖流一体

### 5.1 未开湖

仅 Fluss 侧 per-partition N；无湖同步。

### 5.2 已开湖 / 计划开湖

| 情况 | 行为 |
|------|------|
| 仅新分区用新 N | 新分区 tier 按新 N；旧分区湖布局不变 |
| 旧分区也要新 N | 须 [方案一](./scheme-01-offline-redistribution.md) + Paimon 分区 overwrite |
| 先改 default 后开湖 | 镜像表创建时按 **当时** 各分区 layout |

主键表 per-partition 不同 N：Paimon **Fixed + 分区级 overwrite**；不用 Dynamic/Postpone 作为主路径。

---

## 6. CDC

方案五单独执行 **不产生** `layout_switch`（无整分区 rescale）。新分区首次写入使用新 bucket 集合；CDC 按分区独立推进即可。

若同一表内先后对某旧分区执行方案一，则该分区走方案一 CDC 规则。

---

## 7. `max.bucket.num` 预算

分区表总桶数 = **Σ 各分区 bucketCount**（非 `分区数 × 表级 N`）。

新增高 N 分区前须预检：

```text
sum(existing partition bucketCounts) + N_new <= max.bucket.num
```

---

## 8. 优缺点

| 优点 | 缺点 |
|------|------|
| 实现量小（M1） | 同表多 N，运维与监控复杂 |
| 新分区零迁移 | **无法解救已过热旧分区** |
| 分区间天然隔离 | Flink/Scan 须 per-partition 感知 |
| 符合时间分区衰减 | 全表统一 N 诉求不满足 |

---

## 9. 运维决策

```text
只需未来分区用新 N？
  └─ 是 → 方案五：ALTER defaultBucketCount

旧分区必须改 N？
  └─ 是 → 方案一：rescaleBuckets(partition, N')
         （可与方案五组合：新分区已用 N_new，旧分区单独 rescale）
```

---

## 10. 交付与验收（M1）

| 项 | 内容 |
|----|------|
| 元数据 | `defaultBucketCount`、分区 `bucketCount` |
| RPC | `PartitionInfo` 返回 per-partition N |
| Flink | `BucketLayoutProvider`、Sink/Source per-partition |
| 验收 | 同表两分区不同 N 读写正确；新分区自动 N_new |

---

## 11. 与其他方案

| 方案 | 关系 |
|------|------|
| [方案一](./scheme-01-offline-redistribution.md) | 互补：五改新、一改旧 |
| [方案三](./scheme-03-dynamic-index.md) | 不同模式；三用于在线扩桶非分区级 N 策略 |
| [方案四](./scheme-04-consistent-hashing.md) | 不同模式 |

---

## 参考资料

- [Fluss Partitioning](/table-design/data-distribution/partitioning.md)
- [Fluss Bucketing](/table-design/data-distribution/bucketing.md)
