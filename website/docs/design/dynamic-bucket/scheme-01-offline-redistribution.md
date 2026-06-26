---
title: "Scheme 1 — Offline Partition Redistribution"
sidebar_label: "Scheme 1 Offline Redistribution"
sidebar_position: 1
---

# 方案一：已有分区离线重分布

> **父文档**：[主键表动态分桶 — 架构总览](../dynamic-bucket-rescaling-design.md)  
> **路由范式**：`bucketMode = FIXED_HASH`（`hash(key) % N`）  
> **生产数据面**：**Buckload**（外置 Flink + remote 冷包 + TabletServer 冷加载）  
> **交付**：M2（控制面 + Buckload）；与方案五组合为默认主轨

---

## 1. 方案定义

### 1.1 解决什么问题

在 **固定哈希分桶** 下，当已有分区（或非分区全表）的 `bucketCount` 必须从 `N_old` 变为 `N_new` 时，`hash(key) % N` 对几乎所有主键都会改变路由。必须通过 **全量数据重分布** 保证主键唯一性与 Upsert 语义。

### 1.2 最小操作单元

- **分区表**：单个 `(tableId, partitionId)`
- **非分区表**：`partitionId = null` 表示全表一个逻辑分区

### 1.3 不适用场景

- 仅希望 **未来新分区** 使用新桶数、旧分区不动 → 用 [方案五](./scheme-05-progressive-partition-bucket.md)
- 需要 **不停写** 完成扩缩 → 本方案要求 FENCE（停写停读）；在线路径见 [方案三](./scheme-03-dynamic-index.md)、[方案四](./scheme-04-consistent-hashing.md)
- 期望只改元数据、零迁移 → 主键表 **不可行**（Iceberg PK 禁止 spec 演化同理）

---

## 2. 核心机制

### 2.1 语义概要

```text
FENCE → 读旧 layout（fence 快照）→ 按新 hash % N_new 路由
     → 写入新 layout → VERIFY → SWITCH(layoutEpoch++) → CLEAN
```

| 阶段 | 行为 |
|------|------|
| **FENCE** | 目标范围停写停读；Coordinator 冻结 `fenceSpec` |
| **重分布** | 读取 fence 点前 PK 最终态，写入新桶 |
| **VERIFY** | 行数守恒、路由抽样、checksum |
| **SWITCH** | 原子切换元数据；Bootstrap Log 含 `layout_switch` |
| **CLEAN** | 下线旧桶；可选 LAKE_SYNCING |

**不复制**旧 layout 全量历史 Log；新 layout 下 changelog 从 offset=0 起，CDC 靠 `layout_switch` + `layoutEpoch` 衔接。

### 2.2 与 Paimon Fixed Rescale 的对齐

与 Paimon `ALTER bucket` + 分区 `INSERT OVERWRITE` 同构。已开湖表在 SWITCH 后须 Paimon 侧 overwrite（LAKE_SYNCING），使湖层 bucket 布局与 Fluss 一致。

---

## 3. 数据面实现

### 3.1 Buckload（生产推荐）

Buckload 是方案一的 **推荐生产实现**，不是独立「第六种方案」。与 Lake Tiering 并列的外置 Flink 服务 **`fluss-flink-buckload`**。

| 维度 | Buckload |
|------|----------|
| 读 | **Fluss Client**（`BatchScanner` + bounded `LogScanner`，与 Tiering 读路径同构） |
| 计算 | Flink TaskManager；`keyBy(newBucketId)` |
| 写 | Remote **KvSnapshot 兼容冷包** + **Bootstrap LogSegment** |
| 安装 | Coordinator **`notifyLoadBuckload`** → TS **冷加载**（不走在线 PutKV） |
| 协议 | `buckloadHeartbeat` / `commitBuckloadBundle` / `reportBuckloadLoadComplete` |

#### 3.1.1 读路径

| 表态 | 读法 |
|------|------|
| 有 KV snapshot | snapshot 全量 + log(snapshotOffset → fence) |
| 仅 Log | bounded log(EARLIEST → fence) |

`fenceSpec` 由 Coordinator 在 FENCING 完成后下发；Buckload **不得**越过 fence 点。读负载落在旧桶 Leader，通过 Client 限速与离峰窗口控制。

**建议**：Buckload 的 fence 快照规划与 Tiering `TieringSplitGenerator` **共用同一套边界语义**（或抽公共 `FenceSnapshotPlanner`），避免与 Tiering 读路径不一致导致丢行。

#### 3.1.2 写路径（KvSnapshot 冷包）

冷包采用与线上一致的 **KvSnapshotHandle** 布局：

- **`fluss-client`**：读
- **KV / Log 模块**：Record 与 LogSegment 格式
- **RocksDB**：与 TabletServer **同版本**（根 `pom.xml`）；**RocksDB 打开选项须与 `KvTablet` 同源工厂**，保证冷包可加载

单新桶流程：

1. merge PK 行至 TM 本地 RocksDB
2. Checkpoint → 上传 `kv/` 至 remote staging
3. 生成 Bootstrap LogSegment（含 `layout_switch`）→ 上传 `log/`
4. 最后 PUT `buckload-bundle.json`（`status=COMMITTED`）
5. `commitBuckloadBundle` 登记 Coordinator

#### 3.1.3 Remote 布局与 Manifest-First

复用表级 **`remoteDataDir`**，子树：

```text
{remoteDataDir}/{physicalTablePath}/buckload/{rescaleJobId}/
  bucket-{newBucketId}/attempt-{attemptId}/
    kv/ ...  log/ ...  buckload-bundle.json
```

**禁止**依赖目录原子 `mvdir`。仅 manifest `COMMITTED` + RPC 登记后包可见；TabletServer **禁止**轮询 remote 触发加载。

#### 3.1.4 冷加载（TabletServer）

`notifyLoadBuckload` 推送后：下载 manifest 清单 → 安装 KV（`KvSnapshotDataDownloader`）→ 安装 LogSegment → `reportBuckloadLoadComplete`。全部新桶 LOAD 成功后才 SWITCHING。

### 3.2 集群内重放（备选，非生产默认）

| 维度 | 集群内重放 |
|------|------------|
| 读/写 | TS 本地扫 Log → 在线 Produce 写新桶 |
| 优点 | 无 remote、无 Buckload 服务 |
| 缺点 | 与在线 PutKV 争用；抖动难隔离 |
| 状态机 | RescaleJob 用 `MIGRATING` 替代 `BUCKLOADING`+`LOADING` |
| 建议 | 开发/极小数据/无 remote 降级；**M2 可不交付** |

---

## 4. 控制面：RescaleJob

方案一由 Coordinator **RescaleJob** 编排（Buckload 路径）：

```text
SUBMITTED → VALIDATING → PREPARING → FENCING
  → BUCKLOADING → VERIFYING → LOADING → SWITCHING
  → CLEANING → [LAKE_SYNCING] → COMPLETED
```

详见 [主文档 §9](../dynamic-bucket-rescaling-design.md#9-rescalejob-控制面)。

### 4.1 FENCE 隔离

目标分区：Produce、Lookup、Scan、联合读取 → `PARTITION_RESCALING`。

### 4.2 VERIFYING

1. 各新桶 manifest 已登记，`fenceSpec` 一致  
2. Σ rowCount(旧桶) = Σ rowCount(新桶)  
3. 随机 PK：`hash(pk) % N_new` 与存储位置一致  
4. checksum / 可选 TS 预检  

### 4.3 回滚

| 失败点 | 动作 |
|--------|------|
| BUCKLOADING | 放弃 attempt；不增 layoutEpoch |
| LOADING | 重试 notify 或 ROLLING_BACK |
| SWITCHING 已提交 | 人工介入 |

### 4.4 互斥

与 Rebalance(P)、Tiering(P)、DropPartition(P) 互斥；与 Rescale(其他分区) 可并行。

---

## 5. 元数据

| 字段 | 说明 |
|------|------|
| `layoutEpoch` | SWITCH 时 +1 |
| `rescaleState` | FENCED / BUCKLOADING / LOADING / LAKE_SYNCING |
| `fenceSpec` | 各旧桶 snapshot + log 上界 |
| `committedBundles` | `newBucketId → manifestUri` |

---

## 6. 湖流一体

| 场景 | 行为 |
|------|------|
| 未开湖 | 无 LAKE_SYNCING |
| 先扩缩后开湖 | 推荐；镜像按新 layout 创建 |
| 已开湖且有历史 tier | LAKE_SYNCING：Paimon partition overwrite |
| 联合读取 | FENCE～SWITCHING 拒绝；COMPLETED 后新 epoch |

分桶函数须满足 [主文档 §7](../dynamic-bucket-rescaling-design.md#7-分桶函数与湖层对齐) 规约 R1–R3。

---

## 7. Flink 协同

| 对象 | 要求 |
|------|------|
| **业务 Sink/Source** | rescale 前 `STOP WITH SAVEPOINT`；完成后恢复 |
| **Buckload 作业** | 独立 Flink 集群/作业；`buckloadHeartbeat` 注册 |
| **并行度** | `parallelism >= N_new` 为建议，非硬约束 |

Flink **Key Group** 与 Fluss 桶数是 **独立维度**；方案一改变的是 Fluss `hash % N`，不是 Flink subtask 映射。两层映射关系见 [方案四 §2](./scheme-04-consistent-hashing.md#2-与-flink-key-group-的关系)。

---

## 8. CDC 语义

- FENCE～SWITCH：目标分区无可消费增量（或仅 fence 心跳）
- SWITCH：Bootstrap Log 中 `layout_switch`（`oldEpoch`, `newEpoch`, `newBucketCount`）
- 下游须重置 per-bucket 进度；**官方 CDC 路径须纳入 M2 验收**

---

## 9. 优缺点

| 优点 | 缺点 |
|------|------|
| 正确性最强；与 Paimon 对齐最好 | 分区级停写停读维护窗 |
| Buckload 不占用在线 PutKV | Buckload 读仍打旧桶 IO；LOAD 打新桶磁盘 |
| 可缩桶 | 大数据量耗时长；缩桶成本更高 |
| 复用 remote snapshot 基础设施 | 依赖 remote storage 与 Buckload 服务 |

---

## 10. 交付与验收

| 里程碑 | 内容 |
|--------|------|
| M2 | RescaleJob、Buckload 模块、RPC、冷加载、缩桶 |
| 验收 | 冷包 round-trip IT；开湖/未开湖 rescale；CDC layout_switch；Flink savepoint 协同 |

---

## 11. 与其他方案的关系

| 方案 | 关系 |
|------|------|
| [方案五](./scheme-05-progressive-partition-bucket.md) | **组合**：五管新分区默认 N，一管旧分区强制改 N |
| [方案二](./scheme-02-dual-read-transition.md) | 替代关系；二不停写但正确性/湖对齐差，**不采用** |
| [方案三](./scheme-03-dynamic-index.md) | 不同 `bucketMode`；三在线扩桶、单写者，并列选型 |
| [方案四](./scheme-04-consistent-hashing.md) | 不同 `bucketMode`；四改 N 时迁移量有界，不需整分区 Buckload |

---

## 参考资料

- [Fluss Remote Storage](/maintenance/tiered-storage/remote-storage.md)
- [Paimon Rescale Bucket](https://paimon.apache.org/docs/master/maintenance/rescale-bucket/)
