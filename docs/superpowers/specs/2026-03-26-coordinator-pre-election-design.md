# Coordinator Startup 预选举设计

## 背景

详细的问题分析和证据链见 `docs/superpowers/specs/2026-03-26-replicas-on-offline-deadlock-analysis.md`。

### 问题摘要

Coordinator 重启后，`leader=-1` 且 `ISR=[X]`（单成员）的 bucket 会触发死锁：

1. `ReplicaStateMachine.startup()` 发送 `NotifyLeaderAndIsr(leader=-1)` 给 X
2. X 尝试 `makeFollower(leader=-1)` → `STORAGE_EXCEPTION: Could not find leader`
3. Coordinator 标记 `replicasOnOffline[X] += bucket`
4. `TableBucketStateMachine.startup()` 选举时 `isReplicaOnline(X)` = false → 选举失败
5. 无清除路径 → 永久死锁

300 节点集群每次 coordinator 重启稳定复现，25,497 次失败，8,700+ bucket 卡死。

## 目标

在 `ReplicaStateMachine.startup()` 之前，对 `leader=-1` 且 ISR 单成员的 bucket 执行预选举，确保 NotifyLeaderAndIsr 发送时这些 bucket 已有有效 leader，从根本上消除死锁。

## 设计

### 插入位置

在 `CoordinatorEventProcessor.startup()` 中，`initCoordinatorContext()` 之后、`ReplicaStateMachine.startup()` 之前：

```
initCoordinatorContext()                          ← 加载所有数据，liveSet 完整
  ↓
【新增】recoverLeaderForSingleIsrBuckets()     ← 预选举
  ↓
updateTabletServerMetadataCacheWhenStartup()
  ↓
ReplicaStateMachine.startup()                     ← 此时所有目标 bucket 已有 leader
  ↓
TableBucketStateMachine.startup()
```

此时 `liveTabletServerSet` 已加载完成（300 个 server），`replicasOnOffline` 为空。

### 预选举条件

仅对满足以下**全部**条件的 bucket 执行预选举：
1. `leader == -1`（无 leader）
2. `ISR.size() == 1`（单成员 ISR）
3. ISR 成员在 `liveTabletServerSet` 中

ISR 多成员的 `leader=-1` bucket 走正常的 `TableBucketStateMachine.startup()` 选举路径。

### 选举逻辑

复用 `DefaultLeaderElection`，不新建选举类。调用方构建参数：

```java
List<Integer> assignment = coordinatorContext.getAssignment(bucket);
List<Integer> aliveReplicas = assignment.stream()
        .filter(id -> coordinatorContext.liveTabletServerSet().contains(id))
        .collect(Collectors.toList());

Optional<ElectionResult> result =
        new DefaultLeaderElection().leaderElection(assignment, aliveReplicas, leaderAndIsr);
```

### 预选举方法

在 `CoordinatorEventProcessor` 中新增：

```java
private void recoverLeaderForSingleIsrBuckets() {
    Map<TableBucket, LeaderAndIsr> leaderlessEntries = new HashMap<>();

    // 1. 筛选 leader=-1 且 ISR 单成员的 bucket
    for (Map.Entry<TableBucket, LeaderAndIsr> entry :
            coordinatorContext.bucketLeaderAndIsr().entrySet()) {
        LeaderAndIsr leaderAndIsr = entry.getValue();
        if (leaderAndIsr.leader() == -1
                && leaderAndIsr.isr().size() == 1
                && coordinatorContext.liveTabletServerSet().contains(leaderAndIsr.isr().get(0))) {
            leaderlessEntries.put(entry.getKey(), leaderAndIsr);
        }
    }

    if (leaderlessEntries.isEmpty()) {
        LOG.info("Pre-election: no leaderless single-ISR buckets found");
        return;
    }

    // 2. 对每个 bucket 执行选举
    DefaultLeaderElection election = new DefaultLeaderElection();
    Map<TableBucket, LeaderAndIsr> newLeaderAndIsrMap = new HashMap<>();

    for (Map.Entry<TableBucket, LeaderAndIsr> entry : leaderlessEntries.entrySet()) {
        TableBucket bucket = entry.getKey();
        LeaderAndIsr leaderAndIsr = entry.getValue();
        List<Integer> assignment = coordinatorContext.getAssignment(bucket);
        List<Integer> aliveReplicas = assignment.stream()
                .filter(id -> coordinatorContext.liveTabletServerSet().contains(id))
                .collect(Collectors.toList());

        Optional<ElectionResult> result =
                election.leaderElection(assignment, aliveReplicas, leaderAndIsr);
        if (result.isPresent()) {
            newLeaderAndIsrMap.put(bucket, result.get().leaderAndIsr);
        }
    }

    if (newLeaderAndIsrMap.isEmpty()) {
        LOG.warn("Pre-election: found {} leaderless buckets but none could be elected",
                leaderlessEntries.size());
        return;
    }

    // 3. 批量写 ZK
    zooKeeperClient.updateLeaderAndIsrBatch(newLeaderAndIsrMap);

    // 4. 更新 coordinatorContext
    for (Map.Entry<TableBucket, LeaderAndIsr> entry : newLeaderAndIsrMap.entrySet()) {
        coordinatorContext.putBucketLeaderAndIsr(entry.getKey(), entry.getValue());
    }

    LOG.info("Pre-election: elected leaders for {}/{} leaderless single-ISR buckets",
            newLeaderAndIsrMap.size(), leaderlessEntries.size());
}
```

### ZK 批量写入

需要确认 `ZooKeeperClient` 是否有批量 `updateLeaderAndIsr` 的 API。之前日志中出现过 `"Batch Update LeaderAndIsr"` 日志，说明存在批量接口。如果没有现成的批量方法，需要新增一个。

### 自愈路径

```
Coordinator 重启
  → initCoordinatorContext(): 加载 leader=-1, ISR=[X] 的 bucket
  → recoverLeaderForSingleIsrBuckets():
      X 在 liveSet → leader=X, 批量写入 ZK, 更新 context
  → ReplicaStateMachine.startup():
      发送 NotifyLeaderAndIsr(leader=X) → X makeLeader 成功
      Y, Z makeFollower(leader=X) 成功
  → 无 STORAGE_EXCEPTION → replicasOnOffline 为空
  → TableBucketStateMachine.startup(): 所有目标 bucket 已有 leader
  → 集群自愈
```

### 不变的部分

- `ReplicaStateMachine.startup()` 不变
- `TableBucketStateMachine.startup()` 不变
- `replicasOnOffline` 机制不变
- `processNotifyLeaderAndIsrResponseReceivedEvent` 不变
- `DefaultLeaderElection` 不变
- `ReplicaLeaderElection.java` 不变

### 涉及文件

| 文件 | 变更 |
|------|------|
| `CoordinatorEventProcessor.java` | 新增 `recoverLeaderForSingleIsrBuckets()` 方法，在 `startup()` 中调用 |
| `ZooKeeperClient.java` | 可能需要新增批量 `updateLeaderAndIsr` 方法（如果不存在） |

### 测试策略

1. **单元测试：** 验证预选举在 leader=-1 且 ISR 单成员时正确选主
2. **单元测试：** 验证 leader=-1 但 ISR 多成员时不参与预选举
3. **单元测试：** 验证 ISR 成员不在 liveSet 时不选主
4. **集成测试：** 模拟 coordinator 重启后预选举 → NotifyLeaderAndIsr 成功 → 无死锁

### 风险与缓解

| 风险 | 缓解 |
|------|------|
| 批量 ZK 写入失败 | 预选举失败不影响后续流程，bucket 回退到正常选举路径 |
| ISR 成员实际已不可用（进程在但存储损坏） | 预选举选中后 makeLeader 会失败，触发正常的重新选举 |
| 预选举与 TableBucketStateMachine.startup() 重复选举 | 不会冲突 — 预选举成功的 bucket 在 initializeBucketState 中会被标记为 OnlineBucket，不会进入 triggerOnlineBucketStateChange |
