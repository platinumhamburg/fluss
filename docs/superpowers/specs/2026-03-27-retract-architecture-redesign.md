# Retract Architecture Redesign

## 概述

重新设计 Aggregation Merge Engine 的 retract 支持架构，解决三个架构级缺陷：

1. **AD-1 隐式配对协议** — 当前 retract+upsert 的配对关系在 wire protocol 中没有显式标记，server 依赖位置启发式配对，unpaired retract 会静默损坏数据
2. **AD-2 Checkpoint 活性风险** — Flink 层缓冲 pending retract，checkpoint 时未匹配则抛异常，反压下可能进入 checkpoint 失败循环
3. **AD-3 MergeMode 前向兼容性** — `MergeMode.fromValue()` 对未知值抛 uncaught 异常

## 设计原则

- **每条 record 自描述** — per-record `MutationType` 枚举显式标识 UPSERT / DELETE / RETRACT
- **每条 record 独立处理** — server 端不依赖配对，每条 record 都能独立生成完整 CDC
- **配对是优化，不是正确性依赖** — batch 内相邻同 key retract+upsert 合并处理跳过 intermediate 值，减少 CDC 条数
- **Flink 无状态** — 收到 -U 立即发送 RETRACT record，收到 +U 立即发送 UPSERT record，不缓冲

## 约束

- 不破坏 main 分支已有协议的升级兼容性
- retract 功能仅支持 aggregation merge engine 且整表所有非 PK 聚合列均支持 retract 的表
- 不满足条件的表必须维持过去的语义

---

## 一、Wire Format 变更

### 1.1 PUT_KV API Version 2 的 KvRecord 新格式

```
V0/V1 (现有):  Length(4B) + KeyLength(varint) + Key + Row
V2   (新增):   Length(4B) + MutationType(1B) + KeyLength(varint) + Key + Row
```

注：当前 retract 分支已定义了 PUT_KV V2（用于 request-level `RETRACT_THEN_AGGREGATE`），但该定义仅存在于未合入的 feature 分支，从未发布。本设计重新定义 V2 语义为 per-record MutationType 格式。

### 1.2 MutationType 枚举

新增 `org.apache.fluss.record.MutationType` 枚举（`fluss-common` 模块，与 `DefaultKvRecord` 同包）：

| 值 | 名称 | 语义 |
|----|------|------|
| 0 | UPSERT | 普通 upsert 记录 |
| 1 | DELETE | 显式删除记录 |
| 2 | RETRACT | 撤回记录，携带要撤回的旧值 |

`MutationType` 是 per-record 字段，存在于 KvRecord body 内部。它与 batch/request 级别的 `MergeMode`（DEFAULT/OVERWRITE）是完全独立的命名空间，两者互不干扰。

### 1.3 DELETE 语义在 V2 中的定义

V0/V1 中 DELETE 通过隐式约定表达：Row 为 null（value length = 0）即 DELETE。V2 引入显式 `MutationType.DELETE` 后，两者的关系如下：

- V2 DELETE 记录：`MutationType=DELETE`，Row 必须为 null。如果 Row 非 null，server 忽略 Row 值（防御性处理）。
- V2 UPSERT 记录：`MutationType=UPSERT`，Row 为 null 视为 `InvalidRecordException`（V2 中 UPSERT 必须携带值）。
- V2 RETRACT 记录：`MutationType=RETRACT`，Row 为 null 视为 `InvalidRecordException`（RETRACT 必须携带要撤回的旧值）。
- V0/V1 记录：行为不变，Row 为 null 仍然表示 DELETE。

### 1.4 MergeMode 回滚

- 删除 `RETRACT_THEN_AGGREGATE` 枚举值
- `MergeMode` 保持只有 `DEFAULT` 和 `OVERWRITE`
- `PutKvRequest` 中的 `agg_mode` 字段保持原样（用于 DEFAULT/OVERWRITE 区分）
- Retract 概念完全下沉到 per-record `MutationType`，不再污染 batch 级别

### 1.5 版本门控

- PUT_KV v0/v1：server 用 V0 record 格式解析（无 MutationType 字节）
- PUT_KV v2：server 用 V2 record 格式解析（有 MutationType 字节）
- API 版本协商（`ServerApiVersions`）保证老 server 永远不会收到 v2 请求

### 1.6 Batch 级别

- Batch header `Attributes` 字节不变（仍为 0）
- Batch `magic` 不变（仍为 V0）
- 同一个 batch 内可自由混合 UPSERT、DELETE、RETRACT 记录

### 1.7 DefaultKvRecord V2 实现

**写入（`DefaultKvRecord.writeTo`）：**

新增 V2 重载方法：

```java
public static int writeToV2(OutputView outputView, MutationType mutationType,
                            byte[] key, @Nullable BinaryRow row) throws IOException {
    // 写 Length（占位）
    // 写 MutationType（1 byte）
    // 写 KeyLength + Key + Row（同 V0）
}
```

原有 `writeTo(OutputView, byte[], BinaryRow)` 保持不变，用于 V0/V1。

**读取（`DefaultKvRecord.readFrom`）：**

新增 V2 重载或通过 `ReadContext` 传递 API version：

```java
public static KvRecord readFromV2(MemorySegment segment, int position,
                                   short schemaId, ReadContext readContext) {
    // 读 Length
    // 读 MutationType（1 byte）
    // 读 KeyLength + Key + Row（同 V0）
}
```

**`KvRecord` 接口新增：**

```java
/** 获取 record 的 mutation 类型。V0/V1 record 默认返回 UPSERT（Row 非 null）或 DELETE（Row 为 null）。 */
default MutationType getMutationType() {
    return getRow() == null ? MutationType.DELETE : MutationType.UPSERT;
}
```

**API version 传递路径：**

RPC handler（`TabletService.putKv`）已知请求的 API version → 传递给 `KvTablet.putAsLeader()` → 传递给 `processKvRecords()` → 构造 `ReadContext` 时携带 version 信息 → record iterator 根据 version 选择 V0 或 V2 解析。

### 1.8 Protobuf 变更

`PutKvRequest` protobuf message 本身不变。V2 格式变更完全在序列化的 KvRecordBatch payload 内部。`MergeMode` proto enum 中的 `RETRACT_THEN_AGGREGATE(2)` 从 feature 分支回滚删除（该值从未发布到 main）。

---

## 二、Server 端处理逻辑

### 2.1 核心原则

每条 record 独立处理，配对是纯优化。

### 2.2 Retract 正确性契约

Retract 正确性依赖上游（Flink）发送的 retract 值与之前聚合的值精确匹配。Server 不校验 retract 值是否与历史贡献一致 — 添加校验需要存储 per-key 贡献历史，违背聚合引擎的设计初衷（避免大状态）。如果上游发送了不匹配的 retract 值，聚合状态会被静默损坏。

### 2.3 统一处理流程

`KvTablet.processKvRecords()` 统一处理所有 MutationType：

```
遍历 batch 中的 records:
  mutationType = record.getMutationType()

  switch (mutationType):
    case UPSERT:
      old = read(key)
      new = merge(old, value)
      if old == null:
          生成 +I(new)
      else if new.equals(old):
          跳过（no-change 优化）
      else:
          生成 UB(old) + UA(new)

    case DELETE:
      现有逻辑不变

    case RETRACT:
      old = read(key)
      if old == null:
          跳过（retract 不存在的 key，无操作）
      else:
          // 合并优化：peek 下一条
          if hasNext() && next.key == key && next.mutationType == UPSERT:
              upsertRecord = consumeNext()
              // 两步操作：先 retract 再 merge（复用现有 RowMerger 接口）
              intermediate = currentMerger.retract(old, retractVal)
              new = currentMerger.merge(intermediate, upsertVal)
              if new.equals(old):
                  跳过
              else:
                  生成 UB(old) + UA(new)    // 2条，跳过 intermediate
          else:
              // 独立 retract
              intermediate = currentMerger.retract(old, retractVal)
              if intermediate == null:
                  deleteBehavior = currentMerger.deleteBehavior()
                  if deleteBehavior == IGNORE:
                      跳过
                  else if deleteBehavior == DISABLE:
                      throw DeletionDisabledException
                  else:  // ALLOW
                      apply delete, 生成 -D(old)
              else if intermediate.equals(old):
                  跳过
              else:
                  生成 UB(old) + UA(intermediate)
```

注：合并优化路径中的 `retract` + `merge` 是对现有 `RowMerger.retract()` 和 `RowMerger.merge()` 的顺序调用，不引入新的 `retractThenMerge` 方法。

### 2.4 错误处理

- 收到 RETRACT 但 `RowMerger.supportsRetract()=false` → 抛 `InvalidRecordException`
- 收到 RETRACT 但表不是 aggregation merge engine → 抛 `InvalidRecordException`
- `MutationType` 未知值 → 抛 `InvalidRecordException`（显式拒绝，不静默）

### 2.5 删除的 server 代码

- `processRetractThenAggregateRecords()` 整个方法
- `processRetractThenAggregate()` 整个方法
- `MergeMode` 分发逻辑（`if mergeMode == RETRACT_THEN_AGGREGATE`）

---

## 三、Flink 层变更

### 3.1 UpsertSinkWriter 简化

```java
// 之前：缓冲 -U，等 +U 配对，调用 retractThenUpsert()
// 之后：
case RETRACT:
    upsertWriter.retract(row);   // 直接发送，无缓冲
case UPSERT:
    upsertWriter.upsert(row);    // 直接发送，无配对检查
```

删除：
- `pendingRetractRows` HashMap 及所有相关逻辑
- `flush()` 中的 unmatched retract 检查和 `IOException`
- `close()` 中的 orphaned retract warn 逻辑
- `writeRow()` 中的 RETRACT/UPSERT 配对匹配分支

### 3.2 RowDataSerializationSchema.toOperationType()

分支逻辑不变，RETRACT 分支的下游行为变化（直接发送 vs 缓冲配对）：

```
UPDATE_BEFORE:
  if ignoreDelete         → IGNORE（现有语义，非 agg 表的逃生通道）
  if schemaSupportsRetract → RETRACT（直接映射，不再缓冲）
  if isAggregationTable   → fail-fast（agg 表但有不支持 retract 的函数）
  else                    → DELETE（非 agg PK 表的现有语义）
```

### 3.3 不变的部分

- `FlinkTableSink.getChangelogMode()` — 不变
- `FlinkConversions.computeSchemaSupportsRetract()` — 不变
- 不满足 retract 条件的表 — 行为完全不变

### 3.4 删除的 Flink 代码

- `UpsertWriter.retractThenUpsert()` 接口方法
- `UpsertWriterImpl.retractThenUpsert()` 实现
- `UpsertSinkWriter` 中所有 checkpoint 相关的 retract 逻辑

---

## 四、Client 层变更

### 4.1 WriteRecord

- `mergeMode` 字段保留，但仅用于 `DEFAULT` 和 `OVERWRITE`（`RETRACT_THEN_AGGREGATE` 回滚删除）
- 新增 `mutationType` 字段（`MutationType` 枚举）
- `WriteRecord.forUpsert()` → mutationType=UPSERT
- `WriteRecord.forDelete()` → mutationType=DELETE
- 新增 `WriteRecord.forRetract()` → mutationType=RETRACT

### 4.2 KvWriteBatch

- 删除 `tryAppendPair()` 方法
- `tryAppend()` 正常工作，retract 和 upsert 记录自由混合
- batch-level `mergeMode` 保留（用于 DEFAULT/OVERWRITE 区分）

### 4.3 KvRecordBatchBuilder

- 删除 `hasRoomForPair()`
- `append()` 方法新增 `mutationType` 参数，PUT_KV v2 时写入 1-byte MutationType 到 record 头部

### 4.4 RecordAccumulator

- 删除 `appendPair()` / `appendNewBatchPair()` 及所有 pair 相关逻辑
- `append()` 正常工作，retract record 和 upsert record 走同一条路径
- 不再需要按 MergeMode 拆分 batch（retract 和 upsert 可混合在同一 batch）

### 4.5 WriterClient

- 删除 `sendPair()` / `doSendPair()`
- 删除 `AbstractTableWriter.sendPairWithResult()`
- `send()` 正常工作，retract 就是一条普通的 WriteRecord

### 4.6 UpsertWriter 接口

- 删除 `retractThenUpsert(InternalRow retractRow, InternalRow upsertRow)`
- 新增 `retract(InternalRow row)` — 内部调用 `send(WriteRecord.forRetract(...))`

---

## 五、向后兼容性

| 场景 | 行为 |
|------|------|
| 新 client → 老 server | API 版本协商降级到 v1，不发送 RETRACT record，retract 功能不可用 |
| 老 client → 新 server | server 按 v0/v1 格式解析，无 MutationType 字节，行为不变 |
| 新 client → 新 server | PUT_KV v2，完整 retract 支持 |

**滚动升级：** 客户端按 per-server 协商 API 版本。滚动升级期间，部分 tablet server 可能仍为旧版本。客户端对已升级 server 使用 V2（支持 retract），对未升级 server 降级到 V1（retract 不可用）。建议在升级文档中要求所有 tablet server 升级完成后再启用 retract 功能。

---

## 六、删除代码汇总

| 模块 | 删除项 |
|------|--------|
| fluss-common | `MergeMode.RETRACT_THEN_AGGREGATE` 枚举值；`MergeMode` 相关的所有新增变更回滚到 main |
| fluss-client | `appendPair()`, `appendNewBatchPair()`, `tryAppendPair()`, `hasRoomForPair()`, `sendPair()`, `doSendPair()`, `sendPairWithResult()`, `retractThenUpsert()` |
| fluss-flink | `UpsertSinkWriter.pendingRetractRows` 及配对/flush/close 逻辑；`UpsertWriter.retractThenUpsert()` |
| fluss-server | `processRetractThenAggregateRecords()`, `processRetractThenAggregate()`, `MergeMode` 分发逻辑 |

---

## 七、新增代码汇总

| 模块 | 新增项 |
|------|--------|
| fluss-common | `MutationType` 枚举（UPSERT=0, DELETE=1, RETRACT=2） |
| fluss-common | `DefaultKvRecord` V2 格式读写（`writeToV2` / `readFromV2`，含 MutationType 字节） |
| fluss-common | `KvRecord` 接口新增 `getMutationType()` 默认方法 |
| fluss-client | `WriteRecord.forRetract()` 工厂方法；`WriteRecord.mutationType` 字段 |
| fluss-client | `UpsertWriter.retract(InternalRow)` 接口方法及实现 |
| fluss-server | `processKvRecords()` 中 RETRACT case 处理（独立处理 + 合并优化） |
