# replicasOnOffline 死锁问题分析与设计方向

## 问题概述

Fluss coordinator 每次重启后，大量 bucket（8,700+）永久卡在 OfflineBucket 状态，无法选举 leader。根因是 `replicasOnOffline` 机制在 startup 阶段将健康的 replica 永久标记为 offline，导致后续选举跳过这些 replica，形成死锁。该问题在 300 节点集群上每次 coordinator 重启稳定复现。

## 证据链

### 证据 1：Coordinator startup 加载了全部 300 个 server

诊断日志确认：
```
Got 300 tablet servers from ZK: [0, 1, 2, ..., 299]
Load tablet servers success in 146ms. ZK returned 300 servers, loaded 300 into liveSet,
skipped 0 (null registration), skipped 0 (no endpoint).
```

**结论：** `liveTabletServerSet` 完整，排除 server 遗漏假设。

### 证据 2：选举失败的 ISR 成员在 liveSet 中但在 replicasOnOffline 中

诊断日志确认：
```
ISR member 224 for bucket TableBucket{tableId=178, ...} not in liveReplicas.
inLiveTabletServerSet=true, inReplicasOnOffline=true
```

多个 server（26, 102, 175, 224, 238 等）都是同样的模式：`inLiveTabletServerSet=true, inReplicasOnOffline=true`。

**结论：** 问题不在 liveSet，而在 `replicasOnOffline` 被错误填充。

### 证据 3：NotifyLeaderAndIsr 响应失败的具体错误

诊断日志确认（25,497 次）：
```
NotifyLeaderAndIsr failed for bucket TableBucket{tableId=178, partitionId=2839, bucket=84}
on server 286: ApiError(error=STORAGE_EXCEPTION, message=Could not find leader for follower
replica 286 while make follower for TableBucket{...}.)
```

所有失败都是同一个错误：`STORAGE_EXCEPTION: Could not find leader for follower replica`。

**结论：** 不是 FencedLeaderEpochException，不是 metadata cache 缺失，而是 tablet server 在 `makeFollower` 时找不到 leader。

### 证据 4：失败的 bucket 在 ZK 中 leader=-1

选举失败日志确认：
```
assignment: [224, 3, 0], live replicas: [0],
leaderAndIsr: LeaderAndIsr{leader=-1, leaderEpoch=17, isr=[224], coordinatorEpoch=0, bucketEpoch=18}
```

所有选举失败的 bucket 都是 `leader=-1`，ISR 只有 1 个成员。

**结论：** 这些 bucket 在之前的滚动升级中 ISR 收缩到 1，该成员下线后 leader 被设为 -1。

### 证据 5：Tablet server 没有重启过

用户确认只重启了 coordinator，tablet server 一直在运行。如果仅仅是 UpdateMetadata 时序问题（metadata cache 缺少 server 地址），第一次 coordinator 重启后 tablet server 的 cache 就已经被更新了，后续重启不应该再出现 "Could not find leader" 错误。但实际上每次都稳定复现。

**结论：** "Could not find leader" 不是指 leader server 的网络地址找不到，而是 `leader=-1`（NO_LEADER），tablet server 不知道该从谁 fetch。

### 证据 6：`onReplicaBecomeOffline` 被调用了 4,697 次

搜索 `"The replica ["` 找到 4,697 条日志（之前搜 `"become offline"` 没命中是因为 Set.toString() 展开后文本太长，关键词被推到日志行末尾超出全文索引范围）。

**结论：** `replicasOnOffline` 确实在 startup 期间被大量填充。

## 死锁的完整因果链

```
滚动升级
  → ISR 收缩到 [X]（单成员）
  → X 下线 → ZK 中 leader=-1, ISR=[X]
  → X 重新上线（但 leader 仍为 -1）

Coordinator 重启
  → initCoordinatorContext(): 加载 300 个 server 到 liveSet ✓

  → ReplicaStateMachine.startup():
      → 对所有 replica 发送 NotifyLeaderAndIsr
      → 对 leader=-1 的 bucket，发送 NotifyLeaderAndIsr(leader=-1) 给 X
      → X 尝试 makeFollower(leader=-1) → 失败（没有 leader 可 follow）
      → 返回 STORAGE_EXCEPTION: "Could not find leader"

  → CoordinatorEventManager.start(): 事件线程启动

  → 事件线程处理 NotifyLeaderAndIsrResponseReceivedEvent:
      → processNotifyLeaderAndIsrResponseReceivedEvent()
      → 发现 X 的响应 failed()
      → onReplicaBecomeOffline() → addOfflineBucketInServer(bucket, X)
      → replicasOnOffline[X] += bucket

  → TableBucketStateMachine.startup():
      → initializeBucketState(): bucket 状态为 OfflineBucket（leader=-1）
      → triggerOnlineBucketStateChange(): 尝试选举
      → electLeader(): isReplicaOnline(X, bucket) = false
        （X 在 liveSet 但在 replicasOnOffline 中）
      → 选举失败 → bucket 永久卡在 OfflineBucket

  → 没有任何路径能清除 replicasOnOffline[X] 中的 bucket
    （只有 processNewTabletServer 才清除，但 X 没有重启）
  → 死锁
```

### 关键时序

```
ReplicaStateMachine.startup()     ← 发送 NotifyLeaderAndIsr，产生失败响应
  ↓
CoordinatorEventManager.start()   ← 事件线程启动
  ↓
事件线程处理响应                    ← 标记 replicasOnOffline
  ↓ (可能交错)
TableBucketStateMachine.startup() ← 选举时 replicasOnOffline 已被填充
```

注意：`ReplicaStateMachine.startup()` 在 `CoordinatorEventManager.start()` 之前执行（见 `CoordinatorEventProcessor.startup()` line 265-298），但 NotifyLeaderAndIsr 是异步 RPC，响应通过事件队列处理。事件线程启动后会处理这些积压的响应事件，可能在 `TableBucketStateMachine.startup()` 之前或期间执行。

## `replicasOnOffline` 机制分析

### 数据结构

```java
// CoordinatorContext.java
private final Map<Integer, Set<TableBucket>> replicasOnOffline = new HashMap<>();
// key: serverId, value: 该 server 上被标记为 offline 的 bucket 集合
```

### 写入（1 个入口）

`onReplicaBecomeOffline()` (CoordinatorEventProcessor.java:1011-1035)
- 被 `processNotifyLeaderAndIsrResponseReceivedEvent` 调用
- 当 NotifyLeaderAndIsr 响应中任何 bucket 的 `failed()` 为 true 时触发
- **不区分错误类型** — STORAGE_EXCEPTION、FencedLeaderEpochException、任何错误都一视同仁

### 清除（2 个入口，都是 per-server 粒度）

1. `processNewTabletServer()` (line 1074): `removeOfflineBucketInServer(serverId)` — server 注册时清除
2. `processDeadTabletServer()` (line 1128): `removeOfflineBucketInServer(serverId)` — server 死亡时清除

**没有 per-bucket 的清除路径。**

### 读取（4 个消费者）

1. `ReplicaStateMachine.initializeReplicaState()` — startup 时决定 replica 初始状态
2. `TableBucketStateMachine.initializeBucketState()` — startup 时决定 bucket 初始状态
3. `TableBucketStateMachine.electLeader()` — 选举时构建 liveReplicas
4. `CoordinatorEventProcessor` reassignment — 判断 leader 是否在线

### 设计缺陷

`replicasOnOffline` 混淆了两个不同的概念：

1. **Server 不可达** — server 进程挂了，所有 bucket 都不可用。这由 `processDeadTabletServer` 处理，从 `liveTabletServerSet` 中移除 server。
2. **Bucket 操作失败** — server 活着，但某个 bucket 的 NotifyLeaderAndIsr 失败了。原因可能是瞬态的（leader=-1、metadata cache 过期）或永久的（存储损坏）。

当前代码把这两种情况都往 `replicasOnOffline` 里塞，但清除路径只有 server 重新注册/死亡。**bucket 级别的瞬态失败被当作 server 级别的永久故障处理。**

## `onReplicaBecomeOffline` 的两个动作

```java
private void onReplicaBecomeOffline(Set<TableBucketReplica> offlineReplicas) {
    // 动作 1: 标记 replicasOnOffline（影响 isReplicaOnline）
    for (TableBucketReplica offlineReplica : offlineReplicas) {
        coordinatorContext.addOfflineBucketInServer(
                offlineReplica.getTableBucket(), offlineReplica.getReplica());
    }

    // 动作 2: 如果该 replica 是 leader，触发重新选举
    Set<TableBucket> bucketWithOfflineLeader = new HashSet<>();
    for (TableBucketReplica offlineReplica : offlineReplicas) {
        // ... 检查 leader == offlineReplica
    }
    tableBucketStateMachine.handleStateChange(bucketWithOfflineLeader, OfflineBucket);
    tableBucketStateMachine.handleStateChange(bucketWithOfflineLeader, OnlineBucket);

    replicaStateMachine.handleStateChanges(offlineReplicas, OfflineReplica);
}
```

- **动作 2 是正确的：** 如果 leader 的 NotifyLeaderAndIsr 失败了，确实应该重新选举。
- **动作 1 是问题所在：** 不应该因为一次操作失败就永久标记 replica offline。

## 初步设计方向

### 方向：移除 `processNotifyLeaderAndIsrResponseReceivedEvent` 中对 `replicasOnOffline` 的写入

在 `processNotifyLeaderAndIsrResponseReceivedEvent` 中，对于失败的 bucket：
- **保留** 动作 2：如果失败的 replica 是 leader，触发重新选举
- **移除** 动作 1：不标记 `replicasOnOffline`

这样 leader=-1 的 bucket 的自愈路径就通了：
1. Startup 发送 NotifyLeaderAndIsr(leader=-1) → follower 失败
2. 失败的 replica **不被标记 offline**
3. 选举时 `isReplicaOnline(X)` = true → 选举成功
4. 新 leader 通知发出 → 集群自愈

### 对 `replicasOnOffline` 本身的思考

如果 `processNotifyLeaderAndIsrResponseReceivedEvent` 不再写入 `replicasOnOffline`，那么它的唯一写入者变成了 0。此时可以考虑：
- 直接废除 `replicasOnOffline`
- `isReplicaOnline` 简化为只检查 `liveTabletServerSet`
- Server 级别的 offline 由 `processDeadTabletServer` 从 liveSet 中移除来处理

### 需要审慎评估的风险

1. **如果 STORAGE_EXCEPTION 是真正的存储故障（磁盘损坏等），不标记 offline 会怎样？** 选举可能选中一个存储故障的 replica 为 leader，但后续的 makeLeader 也会失败，产生新的错误，下一轮选举会尝试其他 replica。
2. **`replicasOnOffline` 是否还有其他未发现的用途？** 当前代码分析显示只有 `processNotifyLeaderAndIsrResponseReceivedEvent` 写入，但需要确认是否有其他代码路径。
3. **废除 `replicasOnOffline` 后，`isReplicaOnline` 的语义变化是否影响其他消费者？** 4 个消费者都需要评估。

## 涉及的关键代码文件

| 文件 | 关键行 | 内容 |
|------|--------|------|
| `CoordinatorContext.java` | 104, 162-167, 173-181 | `replicasOnOffline` 定义、`isReplicaOnline()`、`addOfflineBucketInServer()`、`removeOfflineBucketInServer()` |
| `CoordinatorEventProcessor.java` | 988-1035 | `processNotifyLeaderAndIsrResponseReceivedEvent()`、`onReplicaBecomeOffline()` |
| `CoordinatorEventProcessor.java` | 1074, 1128 | `removeOfflineBucketInServer` 在 NewTabletServer/DeadTabletServer 中的调用 |
| `CoordinatorEventProcessor.java` | 265-298 | `startup()` 时序：ReplicaStateMachine → EventManager → TableBucketStateMachine |
| `CoordinatorRequestBatch.java` | 410-443 | `sendNotifyLeaderAndIsrRequest()` — RPC 发送和响应回调 |
| `ReplicaStateMachine.java` | 101-119 | `initializeReplicaState()` — 使用 `isReplicaOnline` |
| `TableBucketStateMachine.java` | 83-103, 657-700 | `initializeBucketState()`、`electLeader()` — 使用 `isReplicaOnline` |
| `ReplicaManager.java` | 543-589, 2198-2232 | `becomeLeaderOrFollower()`、`validateAndGetIsBecomeLeader()` — tablet server 侧处理 |

## 集群现场数据

- 集群：`fluss-pre3-kv-eleme`，300 个 tablet server
- Coordinator 重启时间：2026-03-26 18:28:12
- 选举失败：8,742 个 bucket（OfflineBucket 状态）
- NotifyLeaderAndIsr 失败：25,497 次
- `onReplicaBecomeOffline` 调用：4,697 次
- 所有失败错误类型：`STORAGE_EXCEPTION: Could not find leader for follower replica`
- 所有失败 bucket 的 LeaderAndIsr：`leader=-1`
- 问题稳定复现：每次 coordinator 重启都会发生
