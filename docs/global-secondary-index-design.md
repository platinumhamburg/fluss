# Fluss全局二级索引架构设计

## 概述
Fluss采用**索引表（Index Table）**方式实现全局二级索引，通过独立的物化索引表提供高效的二级索引查询能力。本文档描述了当前实现的完整架构设计和关键特性。

## 设计目标
- ✅ 实现全局二级索引功能，索引列支持多列，支持基于全局二级索引的LookUp操作
- ✅ 支持建表时索引列配置，不允许ALTER TABLE语句修改索引列
- ✅ 可见性控制支持，要求索引列未写入成功前主表数据不可见
- ✅ 索引表创建仅支持主键表

## 核心架构设计

### 整体架构概览

Fluss全局二级索引采用三层架构：
1. **主表层**: 用户数据的主要存储，包含完整的业务数据
2. **索引缓存层**: 主表Leader维护的`IndexCache`，提供索引数据的缓存和服务
3. **索引表层**: 独立的物化索引表，存储索引键到主键的映射关系

### 索引表设计

#### 基本概念
索引表是独立的分布式主键表，用于存储二级索引键到主键的映射关系：
- **独立生命周期**: 索引表的生命周期跟随主表，主表创建/删除时自动创建/删除对应的索引表
- **物化存储**: 索引数据以物化的方式存储在独立的索引表中，支持独立的分片和副本配置
- **异步维护**: 索引数据通过异步复制机制从主表同步，不阻塞主表的写入操作

#### Schema设计详解
索引表的Schema结构如下：
```
索引表列 = 索引列 + 主表主键列 + __offset系统列
索引表主键 = 索引列 + 主表主键列
```

**列顺序规则**：
1. 索引列：按照索引定义中的顺序排列
2. 主表主键列：去重后按照主表中的顺序排列  
3. `__offset`系统列：存储主表WAL中的logOffset值

**主键设计**：
- 组合主键 = 索引列 + 主表主键列
- 确保唯一性：同一个索引键值可能对应多个主表记录
- 支持高效的范围查询和精确查询

#### 命名与约束
- **命名规范**: `"__" + 主表名称 + "__" + "index" + "__" + 索引名称`
- **系统级约束**: 
  - 禁止用户直接对索引表执行DDL操作
  - 禁止用户直接写入索引表数据
  - 索引表仅支持系统内部的查询操作

#### 分区表索引特殊设计

**核心原则**：分区表的索引表是普通表（非分区表），通过动态订阅机制处理分区变化

**设计特点**：
- **索引表类型**: 分区表的索引表始终是普通表，不继承主表的分区特性
- **数据聚合**: 所有分区的索引数据聚合到单个索引表中，便于跨分区查询
- **动态发现**: 索引表通过TabletServerMetadataCache的list and watch机制监听分区变化

**分区监听机制**：
```java
// 索引表动态订阅主表分区变化
TabletServerMetadataCache.watchPartitions(mainTablePath, new PartitionChangeListener() {
    @Override
    public void onPartitionAdded(TablePartition partition) {
        // 为新分区创建索引数据复制链路
        indexFetcherManager.addDataPartition(partition);
    }
    
    @Override
    public void onPartitionRemoved(TablePartition partition) {
        // 停止已删除分区的索引数据复制
        indexFetcherManager.removeDataPartition(partition);
    }
});
```

**复制链路管理**：
- **动态添加**: 新分区创建时，索引表自动建立到新分区的复制链路
- **动态移除**: 分区删除时，自动清理对应的复制链路和状态
- **状态隔离**: 每个分区的复制状态独立管理，互不影响

**查询处理**：
- **统一查询**: 索引查询无需关心数据来源于哪个分区
- **自动路由**: 获取主键后，系统自动路由到正确的分区进行主表查询
- **性能优化**: 索引数据集中存储，避免跨分区扫描的性能开销

### 用户接口设计

#### 建表语法
通过WITH子句定义全局二级索引：
```sql
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,  
    product_id BIGINT,
    amount DECIMAL(10,2),
    PRIMARY KEY (order_id)
) WITH (
  'indexes.columns' = 'user_id;product_id;user_id,product_id',
  'index.bucket.num' = '8'
);
```

**配置说明**：
- `indexes.columns`: 通过';'分隔多个索引配置，索引配置内通过','分隔列
- `index.bucket.num`: 定义索引表的bucket数，影响索引表的并发度和存储分布

#### 索引命名规则
- 自动生成：`idx_${索引列排序列表}`
- 命名约束：只允许字母、数字、下划线，禁止双下划线
- 示例：`user_id`索引 → `idx_user_id`，`user_id,product_id`复合索引 → `idx_product_id_user_id`

### 数据操作流程

#### 查询操作
**主键查询**: 直接访问主表，无需经过索引表
```
用户查询 → 主表KV存储 → 返回结果
```

**二级索引查询**: 两阶段查询模式
```
用户查询 → 索引表查询(获取主键) → 主表查询(根据主键获取完整数据) → 返回结果
```

#### 写入操作时序
数据写入采用异步索引更新机制：
1. **主表写入**: 用户数据写入主表的Leader Replica的WAL和KV存储
2. **索引缓存**: 主表Leader的`IndexCache`实时处理WAL数据，生成索引数据并缓存
3. **索引复制**: 索引表Leader通过`IndexFetcherThread`从主表Leader拉取索引数据
4. **索引应用**: `IndexApplier`将索引数据应用到索引表的WAL和KV存储
5. **可见性控制**: 主表根据索引表的同步状态控制数据可见性

## 核心组件架构

### IndexCache - 主表索引缓存系统

#### 功能定位
`IndexCache`是主表Leader Replica持有的索引缓存组件，采用行级存储+动态组装架构：
- **行级缓存**: 缓存IndexedRow数据而非预组装的LogRecords
- **动态组装**: 根据获取参数动态构建LogRecords  
- **热冷数据融合**: 支持热数据写入和冷数据加载
- **零拷贝优化**: 使用MultiBytesView进行高效内存管理

#### 核心架构
```
IndexCache
├── IndexRowCache                    // 行级缓存管理器
│   └── Map<TableBucket, IndexBucketRowCache>  // 按索引bucket组织
├── IndexCacheWriter                 // 统一缓存写入器  
│   ├── writeHotDataFromWAL()       // 热数据实时写入
│   └── loadColdDataToCache()       // 冷数据按需加载
└── commitHorizonCallback           // 可见性控制回调
```

#### 关键接口
- **fetchIndexLogData()**: 处理索引拉取请求，支持热数据查询和冷数据自动加载
- **writeHotDataFromWAL()**: 实时处理主表WAL记录，生成索引数据并缓存
- **getIndexCommitHorizon()**: 获取当前索引提交水位线，用于可见性控制

### IndexApplier - 索引表数据应用器

#### 功能定位  
`IndexApplier`是索引表Leader Replica持有的索引应用组件，负责将从主表获取的索引数据应用到索引表：
- **状态管理**: 跟踪每个上游数据bucket的索引应用进度
- **原子应用**: 将索引数据写入索引表的WAL和KV存储
- **偏移协调**: 维护数据bucket和索引bucket之间的偏移映射关系

#### 核心状态
```java
// 跟踪每个数据bucket的完整应用状态
Map<TableBucket, IndexApplyStatus> dataBucketStatusMap;

IndexApplyStatus {
    long lastApplyRecordsDataStartOffset;    // 数据bucket地址空间起始偏移
    long lastApplyRecordsDataEndOffset;      // 数据bucket地址空间结束偏移  
    long lastApplyRecordsIndexStartOffset;   // 索引bucket地址空间起始偏移
    long lastApplyRecordsIndexEndOffset;     // 索引bucket地址空间结束偏移
    long indexCommitDataOffset;              // 索引提交进度(数据bucket地址空间)
}
```

#### 关键接口
- **applyIndexRecords()**: 应用索引记录到索引表，支持幂等处理和状态更新
- **getIndexCommitOffset()**: 获取指定数据bucket的索引提交进度
- **onUpdateHighWatermark()**: 基于高水位线推进更新索引提交偏移

### SecondaryIndexLookuper - 二级索引查询器

#### 功能定位
`SecondaryIndexLookuper`实现二级索引的复合查询逻辑：
- **两阶段查询**: 先查询索引表获取主键，再查询主表获取完整数据
- **并发优化**: 支持多个主键的并发查询
- **投影优化**: 自动提取主键字段，减少数据传输

#### 查询流程
```java
CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
    // 阶段1: 查询索引表获取主键列表
    return indexTableLookuper.lookup(lookupKey)
        .thenCompose(indexResult -> {
            // 阶段2: 并发查询主表获取完整数据
            List<CompletableFuture<LookupResult>> mainTableLookups = 
                indexResult.getRowList().stream()
                    .map(indexRow -> primaryKeyProjection.replaceRow(indexRow))
                    .map(primaryKey -> mainTableLookuper.lookup(primaryKey))
                    .collect(toList());
            
            // 阶段3: 合并查询结果
            return CompletableFuture.allOf(mainTableLookups.toArray(new CompletableFuture[0]))
                .thenApply(v -> mergeResults(mainTableLookups));
        });
}
```

### IndexFetcherThread - 索引数据复制器

#### 功能定位
`IndexFetcherThread`负责从主表Leader拉取索引数据并应用到索引表：
- **跨表跨bucket复制**: 支持从多个数据表bucket拉取数据到单个索引表bucket
- **批量处理**: 支持批量拉取和处理索引数据，提高复制效率
- **故障恢复**: 支持复制中断后的自动重试和状态恢复

#### 核心状态管理
```java
// 使用DataIndexTableBucket作为状态key，管理数据表bucket和索引表bucket的映射关系
FairDataIndexTableBucketStatusMap<DataBucketIndexFetchStatus> fairIndexBucketStatusMap;

DataIndexTableBucket {
    TableBucket dataBucket;    // 上游数据表bucket
    TableBucket indexBucket;   // 目标索引表bucket  
}
```

#### 关键流程
1. **构建拉取请求**: 根据IndexApplier的状态构建FetchIndexRequest
2. **批量拉取数据**: 从主表Leader批量获取索引数据
3. **应用索引数据**: 通过IndexApplier将数据应用到索引表
4. **状态协调**: 更新拉取状态和提交进度

### 索引表自动管理

#### IndexTableHelper - 索引表生命周期管理
负责索引表的创建、删除等生命周期管理：
```java
// 主表创建时自动创建索引表
public void createIndexTables(TablePath mainTablePath, TableDescriptor mainTableDescriptor) {
    for (Schema.Index index : mainSchema.getIndexes()) {
        TablePath indexTablePath = IndexTableUtils.generateIndexTablePath(mainTablePath, index.getIndexName());
        TableDescriptor indexTableDescriptor = IndexTableUtils.createIndexTableDescriptor(
            mainTableDescriptor, index, defaultBucketNumber);
        createSingleIndexTable(indexTablePath, indexTableDescriptor);
    }
}
```

**主要功能**：
- 自动创建索引表：根据主表Schema中的索引定义自动创建对应的索引表
- 自动删除索引表：主表删除时自动删除所有关联的索引表  
- 重试机制：支持创建失败时的自动重试和回滚
- 分区同步：支持分区表的索引表分区自动管理

## 数据同步机制

### 异步复制架构

#### 整体流程
Fluss采用异步复制机制确保索引数据的最终一致性：

```
[主表数据写入] → [IndexCache缓存] → [IndexFetcherThread拉取] → [IndexApplier应用] → [索引表存储]
       ↓              ↓                    ↓                    ↓               ↓
   WAL + KV        行级缓存            批量RPC传输         WAL + KV应用      索引数据可见
```

#### 详细步骤
1. **主表写入**: 用户数据写入主表的WAL和KV存储
2. **实时缓存**: IndexCache通过IndexCacheWriter实时处理WAL数据，生成索引数据并缓存
3. **批量拉取**: IndexFetcherThread定期从IndexCache批量拉取索引数据
4. **原子应用**: IndexApplier将索引数据原子地写入索引表的WAL和KV存储
5. **状态反馈**: IndexApplier计算索引提交进度并反馈给主表，用于可见性控制

### 跨表跨bucket复制特性

#### 复制模式
- **一对多映射**: 单个数据表bucket的数据可能分散到多个索引表bucket
- **多对一聚合**: 单个索引表bucket可能接收来自多个数据表bucket的数据
- **状态隔离**: 每个DataIndexTableBucket维护独立的复制状态

#### 分片策略
索引数据的分片基于索引键进行：
```java
// 根据索引键计算目标索引表bucket
int targetBucketId = bucketingFunction.bucketing(indexKeyBytes, indexBucketCount);
```

### 容错机制

#### 故障检测与恢复
- **连接故障**: 自动重试机制处理网络连接问题
- **数据不一致**: 支持重置拉取偏移，从有效位置重新开始
- **Leader切换**: 自动适配Leader变更，重新建立复制链路

#### 幂等性保证
- **重复检测**: IndexApplier支持重复数据的检测和跳过
- **连续性校验**: 严格校验数据偏移的连续性
- **状态恢复**: 支持复制中断后的状态恢复和断点续传

### 分区表索引同步机制

#### 动态分区发现
分区表索引采用动态订阅模式，无需预先知道所有分区：
```java
// IndexFetcherManager中的分区监听实现
public class IndexPartitionWatcher implements PartitionChangeListener {
    @Override
    public void onPartitionAdded(TablePartition partition) {
        LOG.info("Detected new partition {} for index table {}", partition, indexTablePath);
        
        // 为新分区的每个bucket创建IndexFetcherThread
        for (int bucketId = 0; bucketId < partition.getBucketCount(); bucketId++) {
            TableBucket dataBucket = new TableBucket(partition.getTableId(), 
                partition.getPartitionId(), bucketId);
            createIndexFetcherForDataBucket(dataBucket);
        }
    }
    
    @Override
    public void onPartitionRemoved(TablePartition partition) {
        LOG.info("Detected partition removal {} for index table {}", partition, indexTablePath);
        cleanupFetchersForPartition(partition);
    }
}
```

#### 跨分区复制协调
- **独立复制链路**: 每个分区的每个bucket都有独立的IndexFetcherThread
- **统一索引表**: 所有分区的索引数据汇聚到同一个索引表中
- **分区感知状态**: IndexApplier能够区分不同分区的数据并独立管理状态

#### 分区生命周期管理
```java
// 分区创建时的索引同步
CREATE TABLE orders_2024_01 PARTITION OF orders FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
// 系统自动为新分区建立索引复制链路，无需用户干预

// 分区删除时的索引清理
DROP TABLE orders_2024_01;
// 系统自动停止相关的IndexFetcherThread，清理索引数据
```

#### 查询路由优化
分区表索引查询的特殊处理：
1. **索引查询**: 在非分区的索引表中查询，获取主键和`__offset`信息
2. **分区推断**: 根据主键或时间戳等信息推断目标分区
3. **主表查询**: 直接访问推断出的分区，避免全分区扫描

## 可见性控制机制

### 基本原理

#### 分离式可见性设计
Fluss采用分离式可见性控制，确保索引数据同步完成前主表数据不可见：
- **提交与可见分离**: 数据提交到WAL不等于数据可见，需要等待索引同步完成
- **双重水位线**: 使用`highWatermark`和`indexCommitHorizon`联合控制可见性
- **最终一致性**: 保证索引数据与主表数据的最终一致性

#### 水位线管理
```java
// 主表数据可见性边界计算
long visibleWatermark = Math.min(highWatermark, indexCommitHorizon);
```

**核心机制**：
- `highWatermark`: 表示已复制到ISR副本的数据偏移
- `indexCommitHorizon`: 表示索引已同步完成的数据偏移
- 只有同时满足两个条件的数据才对用户可见

### 可见性控制实现

#### IndexCache水位线管理
IndexCache负责维护和更新indexCommitHorizon：
```java
public void updateCommitOffset(Map<TableBucket, IndexCacheFetchParam> fetchRequests) {
    long maxOffset = -1;
    for (Entry<TableBucket, IndexCacheFetchParam> entry : fetchRequests.entrySet()) {
        long commitOffset = entry.getValue().getIndexCommitOffset();
        // 更新每个索引bucket的提交偏移
        indexRowCache.updateCommitOffset(partitionId, bucketId, commitOffset);
        maxOffset = Math.max(maxOffset, commitOffset);
    }
    
    // 触发全局水位线更新
    mayTriggerCommitHorizonCallback(maxOffset);
}
```

#### 主表查询控制
主表在处理查询时会检查可见性边界：
```java
// LogTablet中的查询控制
public LogRecords read(long startOffset, int maxBytes, long maxOffset) {
    // 计算可见边界
    long readMaxOffset = Math.min(maxOffset, getVisibleWatermark());
    return doRead(startOffset, maxBytes, readMaxOffset);
}

private long getVisibleWatermark() {
    if (indexCache != null) {
        return Math.min(highWatermark, indexCache.getIndexCommitHorizon());
    }
    return highWatermark;
}
```

#### KV数据flush控制
KV数据的flush也受到可见性控制：
```java
// KvTablet中的flush控制
public void maybeFlushToKv(long maxOffsetToFlush) {
    // 考虑索引同步状态
    long actualMaxOffsetToFlush = getVisibleFlushOffset(maxOffsetToFlush);
    if (actualMaxOffsetToFlush > lastFlushedOffset) {
        doFlushToKv(actualMaxOffsetToFlush);
    }
}
```

### 状态协调机制

#### 反馈循环
索引系统通过以下反馈循环确保可见性控制：

1. **IndexApplier状态更新**: 索引应用完成后更新indexCommitOffset
2. **IndexFetcherThread状态传递**: 将提交状态传递给主表IndexCache
3. **IndexCache水位线计算**: 基于所有索引bucket状态计算全局水位线
4. **主表可见性更新**: 更新后的水位线立即影响主表数据可见性

#### 精确控制
可见性控制可以精确到LogRecord级别：
- **批量边界处理**: 支持部分batch可见的场景
- **offset精度**: 控制精度达到单个LogRecord的offset级别
- **实时更新**: 水位线变化立即影响查询结果

### 分阶段可见性

#### 初始状态
- 数据写入主表后，indexCommitHorizon初始值为-1
- 此时所有数据均不可见，直到索引同步开始

#### 渐进式推进
- 索引表通过fetchIndexRecords拉取数据
- indexCommitOffset参数驱动水位线渐进推进
- 数据按照索引同步进度逐步变为可见

#### 最终状态
- 所有索引都同步完成后，indexCommitHorizon = highWatermark
- 此时可见性完全由ISR副本同步状态决定

### 异常处理

#### 索引同步滞后
当索引同步出现延迟时：
- 主表数据暂时不可见，直到索引追上
- 提供监控指标显示同步延迟情况
- 支持超时后的降级策略（可配置）

#### 索引表故障
当索引表出现故障时：
- 可见性控制会阻止新数据可见
- 支持临时禁用特定索引的可见性要求
- 故障恢复后自动恢复正常的可见性控制

## API协议与性能优化

### RPC协议设计

#### FetchIndex协议
扩展现有的Fetch协议支持索引数据拉取，通过DataIndexTableBucket管理数据表bucket和索引表bucket的映射关系。

#### 单RPC并行处理
TabletService在单个RPC请求中并行处理日志拉取和索引拉取：
- **数据源分离**: 日志链路和索引链路独立处理
- **批量优化**: 支持批量请求和响应，提高网络效率
- **统一返回**: 在单个FetchLogResponse中返回所有结果

### 性能优化策略

#### 缓存优化
- **行级缓存**: IndexCache采用行级存储，减少内存碎片和数据拷贝
- **智能预读**: 基于访问模式预读相关数据
- **零拷贝**: 使用MultiBytesView避免不必要的数据拷贝

#### 批量处理优化
- **批量拉取**: IndexFetcherThread支持批量拉取多个数据bucket的索引数据
- **批量应用**: IndexApplier支持批量应用索引记录
- **分组优化**: 按主表bucket分组处理请求

#### 并发优化
- **读写分离**: IndexCache使用读写锁，支持高并发读取
- **异步处理**: 索引复制全程异步，不阻塞主表操作
- **并发查询**: SecondaryIndexLookuper支持多个主键的并发查询

## 实现状态总结

### 已完成的核心功能
- ✅ **索引表设计**: 独立的物化索引表，支持自动生命周期管理
- ✅ **用户接口**: 通过DDL语法定义索引，支持多索引和复合索引
- ✅ **数据同步**: 异步复制机制，支持跨表跨bucket复制
- ✅ **查询支持**: SecondaryIndexLookuper实现两阶段查询
- ✅ **可见性控制**: 双重水位线确保数据一致性
- ✅ **缓存系统**: IndexCache提供高效的索引数据缓存
- ✅ **容错机制**: 完整的故障检测、恢复和重试机制
- ✅ **分区表支持**: 分区表索引采用普通表设计，支持动态分区发现和跨分区查询

### 核心组件
1. **IndexCache**: 主表索引缓存系统，支持热冷数据融合
2. **IndexApplier**: 索引表数据应用器，保证幂等性和状态一致性
3. **SecondaryIndexLookuper**: 二级索引查询器，支持高效的复合查询
4. **IndexFetcherThread**: 索引数据复制器，支持批量处理和故障恢复
5. **IndexTableHelper**: 索引表生命周期管理，支持自动创建和删除


## 当前限制和待完善功能

### 数据一致性相关限制

#### 主表Leader切换数据回退问题
**问题描述**：
- 当主表发生Leader切换时，可能出现数据回退的情况（如unclean leader election）
- 主表可以通过truncate机制回退特定bucket的数据到安全的offset
- 但索引表目前缺乏对应的单独truncate特定数据bucket数据的能力

**影响范围**：
- 可能导致索引表数据与主表数据不一致
- 在极端故障场景下需要手动干预或重建索引表


### 状态持久化限制

#### IndexApplier进度持久化缺失
**问题描述**：
- IndexApplier的apply进度（IndexApplyStatus）目前仅存储在内存中
- 索引表Leader重启后会丢失apply进度信息
- 需要从头开始重新构建apply状态，影响恢复效率

**影响范围**：
- 索引表重启后需要较长时间重新同步状态
- 在频繁重启场景下可能影响索引数据的及时性


### 数据生命周期管理限制

#### 索引表TTL机制缺失
**问题描述**：
- 主表的TTL主要依赖分区裁剪机制（删除过期分区）
- 索引表作为普通表（非分区表）无法使用分区裁剪
- 目前缺乏有效的索引表数据TTL清理机制

**影响范围**：
- 索引表数据会无限增长，占用过多存储空间
- 长期运行可能导致性能下降和资源浪费


