---
title: "Scheme 2 — Dual-Read Transition"
sidebar_label: "Scheme 2 Dual-Read"
sidebar_position: 2
---

# 方案二：双读过渡

> **父文档**：[主键表动态分桶 — 架构总览](../dynamic-bucket-rescaling-design.md)  
> **路由范式**：`FIXED_HASH`（迁移期同时存在旧/新两套路由）  
> **产品立场**：**不推荐**；不开湖短窗可评估；开湖表 **禁止**  
> **交付**：无计划实现

---

## 1. 方案定义

### 1.1 核心思想

在 **不停止写入**（或仅短暂软 fence）的前提下，同时维护 **旧 layout** 与 **新 layout** 两套路由：

- **写**：新数据按 `hash(key) % N_new` 写入新桶（或双写旧+新）
- **读**：合并旧桶与新桶结果，按 PK 去重（通常新 layout 优先）

后台异步将旧桶数据清扫至新桶，完成后原子切换元数据、下线旧桶。

### 1.2 要解决的问题

消除或缩短 [方案一](./scheme-01-offline-redistribution.md) 的 **停写停读维护窗**，在「可接受实现复杂度与读放大」的前提下实现在线过渡。

### 1.3 为何 Fluss 默认不采用

| 问题 | 说明 |
|------|------|
| **联合读取** | 湖快照按旧 layout；Fluss 侧双 layout 时，Hybrid merge 几乎无法定义正确语义 |
| **CDC** | 同一 PK 可能在旧/新桶各产生 changelog，下游难去重 |
| **Lookup Join** | 维表缓存与双 layout 不一致风险高于停读 |
| **I1 风险** | 清扫完成前若切换元数据过早，存在双写窗口内 PK 重复可见 |

**结论**：纯 Fluss、无湖、无 CDC、短过渡期的边缘场景可评估；**不作为产品线默认路径**。

---

## 2. 核心机制

### 2.1 写策略变体

| 策略 | 行为 | 写放大 |
|------|------|--------|
| **写新读双**（推荐若实现） | Produce 仅写新 layout；读 merge 旧+新 | 1× |
| **双写** | 同时写旧、新桶 | 2× |
| **写旧读双** | 过渡初期保守方案 | 1×，但新路由延迟生效 |

### 2.2 读合并

点查 / 扫描须：

1. 按 PK 查旧 layout 路由 → 读旧桶  
2. 按 PK 查新 layout 路由 → 读新桶  
3. 按 merge 引擎语义合并（PK 表取最新版本）

扫描全表需枚举 **旧 N_old + 新 N_new** 个桶并去重 PK，**读放大 ≈ 2× 或更高**。

### 2.3 后台清扫

按旧桶并行扫描 → 写入新桶 → 删除旧桶中已迁移 PK。须与在线写 **协调**（per-key fence 或版本号），避免清扫覆盖新写入。

### 2.4 切换

当所有旧桶 PK 已迁移且 in-flight 写已 flush：

- `layoutEpoch++`
- 移除旧 layout 元数据
- 读路径恢复单 layout

---

## 3. 元数据扩展（若实现）

| 字段 | 说明 |
|------|------|
| `dualLayoutState` | `OLD_ONLY` / `TRANSITIONING` / `NEW_ONLY` |
| `oldBucketCount` / `newBucketCount` | 过渡期并存 |
| `migrationWatermark` | 各旧桶清扫进度 |

客户端与 TabletServer 须同时携带 `layoutEpoch` 与 `dualLayoutState` 决定路由。

---

## 4. 与湖流一体

开湖表在过渡期存在：

- Paimon 侧：**单一** bucket 布局（旧 N）
- Fluss 侧：可能双 layout

Union Read 无法保证 PK last-row 正确。**硬约束：lake-enabled 表不得使用方案二。**

---

## 5. CDC 语义

过渡期同一 PK 可能在两个桶各有一条 changelog；须额外 **dedup key = (pk, layoutEpoch)** 或控制事件标记迁移阶段。复杂度高于 [方案一](./scheme-01-offline-redistribution.md) 的 `layout_switch` 单点切换。

---

## 6. Flink 协同

业务作业在过渡期须感知双 layout（BucketLayoutProvider 返回 TRANSITIONING），Sink 写新、Source 读双。比方案一 **更复杂**，且无法通过简单 Savepoint 边界对齐。

---

## 7. 优缺点

| 优点 | 缺点 |
|------|------|
| 理论无长停写窗 | 读放大、清扫与在线写竞争 |
| | CDC / Union Read 难对齐 |
| | 实现与测试复杂度接近方案一 |
| | 长期双 layout 运维负担 |

---

## 8. 与竞品对照

Kafka 式「只增分区、不迁移」不是双读，而是 **放弃路由一致性**。Iceberg 多 spec 共存是 append 语义；PK 表 **禁止**。方案二试图在 PK 语义下做在线过渡，工业界较少采用。

---

## 9. 决策建议

| 场景 | 建议 |
|------|------|
| 生产 PK 表 + 湖 | [方案一](./scheme-01-offline-redistribution.md) |
| 生产 PK 表 + 纯 Fluss | 方案一 |
| 实验性短窗、无 CDC | 可原型验证方案二，不产品化 |
| 在线扩桶、可单写者 | [方案三](./scheme-03-dynamic-index.md) |

---

## 10. 交付状态

**无实现计划**。保留本文档用于架构评审时说明「为何不选双读」及若强需求时的设计边界。
