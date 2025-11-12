# Bucket级别持久化状态管理器设计方案

## 1. 概述

本文档描述了在fluss项目中实现bucket级别持久化状态管理器的设计方案。该状态管理器提供两层存储抽象：底层的bytes模型KV存储和上层的强类型KV存储，与LogTablet有机结合，实现WAL能力和checkpoint机制。

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────┐
│              强类型KV存储抽象层                          │
├─────────────────────────────────────────────────┤
│              KV存储抽象层                        │
├─────────────────────────────────────────────────┤
│        存储实现层 (FileKv/LSMTree/RocksDB)          │
├─────────────────────────────────────────────────┤
│                WAL & Checkpoint                 │
│            (集成到LogSegment)                     │
└─────────────────────────────────────────────────┘
```

### 2.2 包结构设计

```
org.apache.fluss.server.log.state/
├── BucketStateManager.java                // Bucket级别的状态管理器接口
├── encoding/
│   ├── StateKeyEncoder.java              // 状态Key编码器
│   ├── StateValueEncoder.java            // 状态Value编码器
│   └── VersionedValueWrapper.java        // 支持版本演进的Value包装器
├── storage/
│   ├── StateStorage.java           // 存储引擎抽象接口
│   └── DefaultFileStateStorage.java       // 简单文件存储实现
├── wal/
│   ├── StateChangeRecord.java            // 状态变更记录
│   └── StateWALWriter.java               // 状态WAL写入器
├── checkpoint/
│   ├── StateCheckpoint.java              // 状态检查点
│   ├── StateCheckpointManager.java       // 检查点管理器
│   └── StateRecoveryManager.java         // 状态恢复管理器
└── recovery/
    └── StateReplayer.java           // 状态重放引擎
```

## 3. 核心接口设计

### 3.1 状态管理器接口

```java
// StateManager.java - 状态管理器主接口
public interface BucketStateManager {
    <K, V> TypedStateStore<K, V> getOrCreateStore(StateDescriptor<K, V> descriptor);
    void checkpoint(long logOffset) throws IOException;
    void recover(long fromOffset, long toOffset) throws IOException;
    void close() throws IOException;
}
```

### 3.2 存储层接口

```java
// RawStateStore.java - 底层bytes KV存储
public interface RawStateStore {
    void put(byte[] key, byte[] value) throws IOException;
    byte[] get(byte[] key) throws IOException;
    void delete(byte[] key) throws IOException;
    CloseableIterator<Entry<byte[], byte[]>> iterator() throws IOException;
}

// TypedStateStore.java - 强类型KV存储
public interface TypedStateStore<K, V> {
    void put(K key, V value) throws IOException;
    V get(K key) throws IOException;
    void delete(K key) throws IOException;
    CloseableIterator<Entry<K, V>> iterator() throws IOException;
}
```

### 3.3 存储引擎接口

```java
// StateStorageEngine.java - 存储引擎抽象
public interface StateStorage extends RawStateStore {
    void flush() throws IOException;
    void compact() throws IOException;
    long estimateSize();
    StateSnapshot createSnapshot() throws IOException;
    void restoreFromSnapshot(StateSnapshot snapshot) throws IOException;
}
```

## 4. WAL集成设计

### 4.1 LogSegment文件格式扩展

在现有LogSegment文件格式基础上，追加State变更日志存储区域：

```
LogSegment文件结构:
┌─────────────────────┐
│   LogRecordBatch 1  │  <- 现有数据区域
├─────────────────────┤
│   LogRecordBatch 2  │
├─────────────────────┤
│        ...          │
├─────────────────────┤
│   StateChangeLog    │  <- 新增状态变更日志区域
│   - Batch Index     │     - 记录所属的Batch索引
│   - Offset Index    │     - 记录在Batch中的offset
│   - State Changes   │     - 具体的状态变更内容
└─────────────────────┘
```

### 4.2 状态变更日志格式

```java
// StateChangeRecord.java - 状态变更记录
public class StateChangeRecord {
    private final long batchIndex;      // 关联的LogRecordBatch索引
    private final int offsetInBatch;    // 在Batch中的offset位置
    private final String namespace;     // 命名空间
    private final byte[] key;          // 状态key
    private final byte[] value;        // 状态value（null表示删除）
    private final long timestamp;      // 变更时间戳
    private final byte operationType;  // 操作类型：PUT/DELETE
}
```

### 4.3 WAL写入流程

```java
// StateWALWriter.java - 状态WAL写入器
public class StateWALWriter {
    // 与LogSegment写入同步，确保一致性
    public void writeStateChange(
        long batchIndex, 
        int offsetInBatch, 
        StateChangeRecord record
    ) throws IOException;
    
    // 在LogSegment rolling时触发
    public void flush() throws IOException;
}
```

## 5. 编码方案设计

### 5.1 Key编码格式

```
State Key格式:
┌──────────────┬──────────────┬──────────────┬──────────────┐
│   Namespace  │  Version(1B) │  Key Length  │   User Key   │
│     (8B)     │              │     (4B)     │   (Variable) │
└──────────────┴──────────────┴──────────────┴──────────────┘
```

### 5.2 Value编码格式

```
State Value格式:
┌──────────────┬──────────────┬──────────────┬──────────────┐
│   Version    │  Schema ID   │ Value Length │  User Value  │
│     (2B)     │     (2B)     │     (4B)     │  (Variable)  │
└──────────────┴──────────────┴──────────────┴──────────────┘
```

### 5.3 命名空间设计

```java
// NamespaceEncoder.java - 命名空间编码
public class NamespaceEncoder {
    // 支持层次化命名空间：table.bucket.operator.state
    public static long encodeNamespace(
        long tableId, 
        int bucketId, 
        String operatorName, 
        String stateName
    );
    
    public static NamespaceInfo decodeNamespace(long encodedNamespace);
}
```

## 6. Checkpoint机制设计

### 6.1 Checkpoint触发时机

- LogSegment rolling时自动触发
- 手动触发（管理接口）

### 6.2 Checkpoint文件格式

```
StateCheckpoint文件结构:
┌─────────────────────┐
│    Header           │
│  - Version: 4B      │
│  - Offset: 8B       │
│  - Timestamp: 8B    │
│  - CRC: 4B          │
├─────────────────────┤
│   Metadata          │
│  - Namespace Count  │
│  - Total Key Count  │
├─────────────────────┤
│   Data Blocks       │
│  - Namespace 1      │
│    - Key-Value...   │
│  - Namespace 2      │
│    - Key-Value...   │
└─────────────────────┘
```

### 6.3 恢复流程

1. 加载最新的StateCheckpoint
2. 从checkpoint对应的LogSegment开始重放WAL
3. 应用状态变更到内存状态
4. 重建索引和缓存

## 7. 存储实现方案

### 7.1 SimpleFileStateStorage（简单文件存储）

- 基于文件的简单KV存储
- 适用于小规模状态
- 实现简单，性能一般


## 8. 配置参数设计

```properties
# 状态管理器配置
fluss.bucket.state.storage.engine=file  # 存储引擎选择
```

## 9. 与LogTablet集成

### 9.1 生命周期管理

```java
// 在LogTablet中集成StateManager
public class LogTablet {
    private final BucketStateManager stateManager;
    
    // LogTablet创建时初始化StateManager
    public static LogTablet create(...) {
        BucketStateManager stateManager = new BucketStateManager(...);
        // ... 其他初始化逻辑
    }
    
    // 在LogSegment rolling时触发checkpoint
    private void rollSegment() {
        // ... 现有rolling逻辑
        stateManager.checkpoint(getCurrentLogEndOffset());
    }
}
```

### 9.2 故障恢复集成

```java
// 在LogTablet恢复过程中恢复状态
public void recover(long fromOffset, long toOffset) {
    // ... 现有恢复逻辑
    stateManager.recover(fromOffset, toOffset);
}
```

## 10. 监控和度量

### 10.1 关键指标

- 状态存储大小
- 读写QPS和延迟
- Checkpoint频率和耗时
- WAL写入量
- 恢复时间

### 10.2 JMX指标

```java
// 在BucketMetricGroup中添加状态管理器指标
public class BucketMetricGroup {
    private void registerStateMetrics(BucketStateManager stateManager) {
        // 注册状态相关的监控指标
    }
}
```

## 11. 实现计划

### Phase 1: 核心框架
1. 实现基础接口定义
2. 实现SimpleFileStateStorage
3. 实现基础的编码/解码功能
4. 基础单元测试

### Phase 2: WAL集成
1. 扩展LogSegment文件格式
2. 实现StateWALWriter
3. 集成到LogTablet生命周期
4. 集成测试

### Phase 3: 高级功能
1. 实现LSMTreeStateStorage或RocksDBStateStorage
2. 完善Checkpoint机制
3. 实现状态恢复功能
4. 性能优化

### Phase 4: 生产就绪
1. 完善监控和度量
2. 压力测试和性能调优
3. 文档完善
4. 生产环境验证

## 12. 注意事项

1. **线程安全**: 所有公共接口必须是线程安全的
2. **资源管理**: 正确管理文件句柄、内存等资源
3. **错误处理**: 完善的异常处理和错误恢复机制
4. **向后兼容**: 编码格式支持版本演进
5. **性能考虑**: 避免在关键路径上引入性能瓶颈
6. **配置灵活性**: 支持不同场景下的配置调优

## 13. 风险评估

1. **复杂性风险**: 设计较为复杂，需要careful实现
2. **性能风险**: WAL写入可能影响写入性能
3. **兼容性风险**: 文件格式变更需要向后兼容
4. **稳定性风险**: 新增组件可能引入稳定性问题

## 14. 缓解方案

1. 分阶段实现，逐步验证
2. 充分的单元测试和集成测试
3. 性能基准测试和压力测试
4. 提供配置开关，支持渐进式部署
