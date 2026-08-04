# 弹内二级索引 V2 独立镜像设计

## 1. 背景

本次工作以 Aone CI 流水线 `142673` 的成功运行 `57762814` 为构建基线，将完整的全局二级索引 V2 能力迁移到 Fluss `0.9-ali-5.0` 弹内版本，并发布一个独立镜像。

基线构成如下：

- Fluss：`release-0.9-ali-5.0@d0b5639691e638b79ffffd57848313e186686b54`
  - 基线运行使用的 `release-0.9-ali-5.0-1` 指向同一提交。
- fluss-ali：`release-0.9-ali-5.0-ali@0b16fc415663f2bf6afe55c0668469e630fc0950`
- Aone CI：`构建弹内镜像V2`，pipeline ID `142673`
- 基线镜像：`turbo-x-registry.cn-zhangjiakou.cr.aliyuncs.com/fluss/fluss:release-0.9-ali-5.0-1`

二级索引的权威功能与兼容性定义来自 `fip/FIP-Global-Secondary-Index-V2.md`。

## 2. 目标

- 迁移完整二级索引 V2，包括：
  - Index Table 生命周期；
  - IndexReplicator、IndexSendBuffer 和 IndexSender 复制链路；
  - sync/async 可见性；
  - 累计进度 WriterState 与 PutKv API v2；
  - 非分区表与分区表索引；
  - PartitionTombstone 与 native compaction filter；
  - Java SecondaryIndexLookuper；
  - Flink SQL DDL 与同步、异步 Lookup。
- 使用独立的 `fluss-index` Catalog identifier，使索引版 connector 能与普通 `fluss` connector 并存。
- 复用现有 pipeline `142673` 构建并发布独立弹内镜像。
- 保持原 `release-0.9-ali-5.0`、`release-0.9-ali-5.0-ali` 和既有镜像不变。

## 3. 不在范围内

- 不迁移历史 fluss-ali index 分支中的 DeltaJoin 测试。
- 不迁移历史 Catalog 索引注入实现；该逻辑已经由 V2 核心 `FlinkCatalog.wrapWithIndexes()` 统一提供。
- 不新增 Aone pipeline，也不改变 pipeline `142673` 的构建阶段。
- 不部署镜像到运行中的 Fluss 集群。
- 不支持在线 `ADD INDEX`、`DROP INDEX` 或已有表索引回填。
- 本次内部镜像只交付 Linux amd64 native library，不承诺 macOS、Windows、Linux aarch64 等平台的发布产物。

## 4. 分支与版本

### 4.1 RocksDB

- 仓库：`https://github.com/platinumhamburg/rocksdb`
- 特性分支：`feature/floor-set-compaction-filter`
- 当前特性提交：
  - `5b6dd34`：增加 FloorSet compaction filter、JNI 和 Java API；
  - `a00b8ab`：加强短 value 等边界处理；
  - `ac10bdc`：按 Big-Endian 解码 valueTag。
- Maven 坐标：
  - groupId：`io.github.platinumhamburg`
  - artifactId：`rocksdbjni-hidden`
  - version：`11.5.0-index-v2-20260804-SNAPSHOT`
- native base name：`rocksdbhidden`，生成 `librocksdbhiddenjni-linux64.so`。

特性分支必须先发布到 GitHub，后续构建必须通过 branch checkout 获得源码，不能依赖本地未发布提交。

### 4.2 Fluss

- 基线：`release-0.9-ali-5.0@d0b5639`
- 目标分支：`release-0.9-ali-5.0-index-v2`
- Maven 版本：`0.9-ali-5.0-index-v2`

目标分支定向迁移以下提交，顺序保持不变：

1. `9d14e35af`：完整二级索引 V2 squash 实现；
2. `afc558058`：拒绝以下划线开头的索引名；
3. `149b0ab47`：IndexSendBuffer 和 IndexReplicationSupervisor 命名整理；
4. `5337298d4`：解耦 IndexSendBuffer 与 IndexReplicator。

不迁移源分支基线上的两个无关 main 提交 `92b8edc36` 和 `1ca13b429`。

Fluss 的 dependencyManagement 与 `fluss-common` 依赖从标准 `com.ververica:frocksdbjni` 切换为本设计中的 `rocksdbjni-hidden` Snapshot。运行时只加载独立命名的 native library，避免与 Flink/VVR 自带的 `frocksdbjni` 发生 native library 名称冲突。

### 4.3 fluss-ali

- 基线：`release-0.9-ali-5.0-ali@0b16fc4`
- 目标分支：`release-0.9-ali-5.0-index-v2`
- Maven 版本：`0.9-ali-5.0-index-v2`

只迁移历史提交 `4651da0` 的行为：

```java
FlussAliCatalogFactory.IDENTIFIER = "fluss-index";
```

如果原提交能干净应用，则直接 cherry-pick；如果因上下文变化无法应用，则只手工实现这一行等价变更。历史分支的其他生产代码、Paimon shading、VVR 版本变更和测试均不迁移。

两个目标分支使用 `release-*` 命名，是用户针对本次发布明确指定的分支命名例外。

## 5. 构建与发布流程

### 5.1 本地构建并发布 RocksDB JNI

1. 从 `platinumhamburg/rocksdb` checkout `feature/floor-set-compaction-filter`。
2. 以 Linux amd64 为目标构建 RocksDB JNI。
3. 只导出 JNI 符号，并把 native base name 改为 `rocksdbhidden`。
4. 运行 C++ FloorSet filter 测试和 Java JNI 测试。
5. 组装包含 Java classes 与 `librocksdbhiddenjni-linux64.so` 的 JAR。
6. 发布 `11.5.0-index-v2-20260804-SNAPSHOT` 到内部 Maven Snapshot 仓库。
7. 使用一个干净 Maven 本地仓库重新解析该坐标，证明 CI 能独立下载。

RocksDB Maven 发布在本地完成，不增加 RocksDB Aone pipeline。

### 5.2 构建 Fluss 与 fluss-ali 分支

1. 从 Fluss 基线创建目标分支并定向迁移四个 V2 提交。
2. 更新 RocksDB JNI Maven 坐标。
3. 完成 Fluss 验证后提交并推送目标分支到内部 GitLab。
4. 从 fluss-ali 基线创建同名目标分支。
5. 迁移 `fluss-index` identifier，完成 package 验证后提交并推送。

任何提交或推送都必须在执行前取得用户明确确认；禁止 force push。

### 5.3 复用 pipeline 142673

正式触发参数固定为：

```text
FLUSS_GIT_URL=git@gitlab.alibaba-inc.com:fluss/fluss.git
FLUSS_GIT_BRANCH=release-0.9-ali-5.0-index-v2
FLUSS_ALI_GIT_BRANCH=release-0.9-ali-5.0-index-v2
VERSION=0.9-ali-5.0-index-v2
```

流水线数据流如下：

1. checkout Fluss 目标分支；
2. 从内部 Maven 仓库解析 `rocksdbjni-hidden`；
3. 编译并 install Fluss；
4. checkout fluss-ali 目标分支；
5. 使用同一 Maven 版本打包 fluss-ali；
6. 使用现有 `docker-inner` 资产构建并推送镜像。

最终镜像：

```text
turbo-x-registry.cn-zhangjiakou.cr.aliyuncs.com/fluss/fluss:release-0.9-ali-5.0-index-v2
```

## 6. 验证设计

### 6.1 RocksDB

- C++ `floor_set_compaction_filter_test` 通过。
- Java `FloorSetCompactionFilterTest` 通过。
- JAR 包含 `org/rocksdb/FloorSetCompactionFilter.class`。
- JAR 包含 `librocksdbhiddenjni-linux64.so`，不包含冲突名称的 `librocksdbjni-linux64.so`。
- native library 只导出 JNI 函数，不导出 RocksDB C++ vtable。
- 从内部 Maven Snapshot 仓库重新下载的 JAR checksum 与发布产物一致。

### 6.2 Fluss

先运行受影响模块及已有索引测试，再执行工作区标准验证：

```bash
mvn clean compile -o -Dmaven.repo.local=../fluss-index-v2/.cache \
  -pl !fluss-lake/fluss-lake-lance,!fluss-dist

mvn test -o -Dmaven.repo.local=../fluss-index-v2/.cache \
  -pl !fluss-lake/fluss-lake-lance,!fluss-dist
```

离线模式因新发布 Snapshot 尚未缓存而失败时，允许临时去掉 `-o` 下载依赖，再恢复离线验证。重点确认现有 V2 测试，包括复制、乱序、failover、远端 WAL 恢复、目标恢复、Java lookup、Flink lookup 和 native compaction filter。

### 6.3 fluss-ali

- Maven dependency resolution 成功。
- 现有目标模块 package 成功。
- Flink Factory discovery 能发现 identifier `fluss-index`。
- 不要求迁移或运行历史 index 分支的 DeltaJoin 测试。

### 6.4 CI 与镜像

1. 使用 pipeline `142673` dry-run 校验分支和参数。
2. 正式运行中 `build-fluss`、`package-fluss-ali` 和 `build-image` 全部成功。
3. 记录最终镜像 URL 与 digest。
4. 验证镜像文件系统中包含目标 Fluss 版本与 `rocksdbjni-hidden` JAR。

## 7. 失败处理

- RocksDB native 或 Java 测试失败：不发布 Maven Snapshot。
- Maven Snapshot 无法从干净本地仓库解析：不开始 Fluss 迁移验证。
- Fluss compile/test 失败：不提交、不推送、不触发 CI。
- fluss-ali package 或 Factory discovery 失败：不推送 fluss-ali 分支。
- pipeline 失败：读取所有失败 Job/step 日志并定位根因，不直接重跑。
- 目标镜像 tag 已存在：不得覆盖，改用递增版本 `release-0.9-ali-5.0-index-v2-2`，并同步把 Maven VERSION 改为 `0.9-ali-5.0-index-v2-2`。

## 8. 回滚与兼容边界

- 本次范围止于构建和发布镜像，不授权部署。
- 创建第一张带二级索引的表之前，可以直接恢复原 `release-0.9-ali-5.0` 镜像。
- 一旦创建累计进度模式的 Index Table，就不能降级到不理解新 KV、WAL 和 WriterState 格式的旧镜像。
- 普通 KV Table 继续使用既有连续 batch sequence 模式；不含索引的表不改变恢复语义。
- 升级实际集群时必须先升级所有可能承载 Index Bucket 的 TabletServer，再升级 Coordinator，最后升级 Java client 与 Flink connector；该部署流程不在本次实施范围内。

## 9. 完成标准

- RocksDB JNI Snapshot 已发布且可从内部 Maven 仓库独立解析。
- Fluss 与 fluss-ali 两个目标分支都基于约定基线并仅包含约定增量。
- Fluss 完整验证、fluss-ali package 与 Factory discovery 通过。
- pipeline `142673` 成功完成。
- 独立镜像及 digest 已记录。
- 未修改任何备份分支，未 force push，未部署镜像。
