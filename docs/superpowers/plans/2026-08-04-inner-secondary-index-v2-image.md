# Inner Secondary Index V2 Image Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 基于 Fluss `release-0.9-ali-5.0` 与 fluss-ali `release-0.9-ali-5.0-ali`，发布包含完整二级索引 V2 和分区 TTL native compaction filter 的独立弹内镜像。

**Architecture:** RocksDB fork 的 `feature/floor-set-compaction-filter` 作为唯一 native 源，构建 Linux amd64、隐藏 C++ 符号且使用独立 native base name 的 JNI Snapshot。Fluss 目标分支只迁移四个 V2 提交并切换 JNI Maven 坐标；fluss-ali 目标分支只暴露 `fluss-index` Catalog identifier；最后复用 Aone pipeline `142673` 构建镜像。

**Tech Stack:** Java 11、Maven 3.9、RocksDB 11.5.0、C++/JNI、Docker Linux amd64、Flink/VVR、Aone CI/opencli、Git/Git worktree。

## Global Constraints

- RocksDB 源仓库固定为 `https://github.com/platinumhamburg/rocksdb`，源分支固定为 `feature/floor-set-compaction-filter`，确认的 tip 为 `ac10bdc`。
- RocksDB Maven 坐标固定为 `io.github.platinumhamburg:rocksdbjni-hidden:11.5.0-index-v2-20260804-SNAPSHOT`。
- native base name 固定为 `rocksdbhidden`；发布 JAR 必须包含 `librocksdbhiddenjni-linux64.so`，不得包含 `librocksdbjni-linux64.so`。
- 本次只交付 Linux amd64 native 产物；Fluss 的 native 相关测试必须在 Linux amd64 环境运行。
- Fluss 基线固定为 `release-0.9-ali-5.0@d0b5639691e638b79ffffd57848313e186686b54`。
- Fluss 目标分支固定为 `release-0.9-ali-5.0-index-v2`，只迁移 `9d14e35af`、`afc558058`、`149b0ab47`、`5337298d4`，顺序不得改变。
- fluss-ali 基线固定为 `release-0.9-ali-5.0-ali@0b16fc415663f2bf6afe55c0668469e630fc0950`，目标分支固定为 `release-0.9-ali-5.0-index-v2`。
- `release-*` 是用户针对本次发布批准的分支命名例外；不得借此创建其他不符合 `feature/` 或 `fix/` 约定的开发分支。
- 构建 Maven 版本固定为 `0.9-ali-5.0-index-v2`；由本地临时 `versions:set` 和 CI 参数设置，不做无关的全仓 POM 版本提交。
- fluss-ali 只迁移 `FlussAliCatalogFactory.IDENTIFIER = "fluss-index"` 的行为；不迁移历史 DeltaJoin 测试、Catalog 索引注入、Paimon shading 或 VVR 版本改动。
- Aone pipeline 固定为 project `fluss-ali` 的 pipeline `142673`；不得修改 pipeline 文件或新增流水线。
- 正常目标镜像固定为 `turbo-x-registry.cn-zhangjiakou.cr.aliyuncs.com/fluss/fluss:release-0.9-ali-5.0-index-v2`。
- 目标 tag 已存在时不得覆盖；改用 Fluss 别名分支 `release-0.9-ali-5.0-index-v2-2`、Maven 版本 `0.9-ali-5.0-index-v2-2` 和同名镜像 tag。
- 禁止 force push。每组 commit 和每次 push 执行前都必须获得用户对明确范围的确认。
- 不部署镜像，不修改运行中的 Fluss 集群。

---

## File and Artifact Map

- RocksDB 已有功能文件：
  - `utilities/compaction_filters/floor_set_compaction_filter.h`：Floor + explicit set 过滤逻辑与 Big-Endian tag 解码。
  - `utilities/compaction_filters/floor_set_compaction_filter_test.cc`：C++ 边界与编码测试。
  - `java/rocksjni/floor_set_compaction_filter_jni.cc`：JNI 桥接。
  - `java/src/main/java/org/rocksdb/FloorSetCompactionFilter.java`：Java API。
  - `java/src/test/java/org/rocksdb/FloorSetCompactionFilterTest.java`：Java/JNI 行为测试。
  - `CMakeLists.txt`、`Makefile`、`src.mk`、`java/CMakeLists.txt`、`java/Makefile`：源码和测试注册。
- RocksDB 生成产物：
  - `/Users/wangyang/.cache/rocksdbjni-build/dist/rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar`：待发布 fat JAR。
  - JAR 内 `librocksdbhiddenjni-linux64.so`：隐藏 C++ 导出的 Linux amd64 native library。
- Fluss V2 迁移文件集：精确等于 `git diff --name-status 1ca13b429c9679807406d8b7b9bdbb537b63070c..5337298d4` 得到的 277 个路径；这些路径只通过四个指定提交迁移，不逐文件手工重写。
- Fluss 手工修改文件：
  - `pom.xml`：管理 `rocksdbjni-hidden` 坐标与版本。
  - `fluss-common/pom.xml`：把运行时依赖从 `frocksdbjni` 切换到 `rocksdbjni-hidden`。
- fluss-ali 手工/定向迁移文件：
  - `fluss-ali-flink/fluss-ali-vvr-common/src/main/java/org/apache/fluss/flink/catalog/FlussAliCatalogFactory.java`：将 Catalog identifier 改为 `fluss-index`。
  - `fluss-ali-flink/fluss-ali-vvr-common/src/test/java/org/apache/fluss/flink/catalog/FlussAliTableFactoryTest.java`：增加 identifier 合同测试；不引入 DeltaJoin 测试。
- CI 文件 `.aoneci/build-image-inner-v2.yaml` 仅作为 pipeline `142673` 的既有定义读取，禁止修改。

### Task 1: Establish Reproducible Branch and Workspace Preconditions

**Files:**
- Read: `docs/superpowers/specs/2026-08-04-inner-secondary-index-v2-image-design.md`
- Read: `fip/FIP-Global-Secondary-Index-V2.md`
- Read: `.aoneci/build-image-inner-v2.yaml` from fluss-ali baseline
- Modify: none

**Interfaces:**
- Consumes: 已批准设计、三个本地仓库、指定基线提交。
- Produces: 干净且可复核的 RocksDB、Fluss、fluss-ali 起点；后续任务不得绕过这些断言。

- [ ] **Step 1: Re-read the authoritative design and FIP before implementation**

Run:

```bash
sed -n '1,260p' /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree/docs/superpowers/specs/2026-08-04-inner-secondary-index-v2-image-design.md
sed -n '1,1200p' /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fip/FIP-Global-Secondary-Index-V2.md
```

Expected: 设计包含完整 V2、PartitionTombstone、`fluss-index`、pipeline `142673` 和不部署边界；FIP 中的数据格式、复制进度与查询语义没有被计划外缩减。

- [ ] **Step 2: Verify every repository is clean before switching or creating branches**

Run each command in its repository:

```bash
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/rocksdb status --short --branch
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree status --short --branch
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali status --short --branch
```

Expected: RocksDB is on `feature/floor-set-compaction-filter`; Fluss is on `index-v2-rebase-squash`; all three working trees have no uncommitted paths. Any unexpected path is user-owned and stops branch operations until reviewed.

- [ ] **Step 3: Verify immutable source commits**

Run:

```bash
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/rocksdb rev-parse HEAD
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree rev-parse d0b5639691e638b79ffffd57848313e186686b54
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree show -s --format='%H %P %s' 9d14e35af afc558058 149b0ab47 5337298d4
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali rev-parse origin/release-0.9-ali-5.0-ali
```

Expected:

```text
RocksDB HEAD = ac10bdc
Fluss base = d0b5639691e638b79ffffd57848313e186686b54
V2 chain = 9d14e35af -> afc558058 -> 149b0ab47 -> 5337298d4
fluss-ali base = 0b16fc415663f2bf6afe55c0668469e630fc0950
```

- [ ] **Step 4: Check that target remote branches do not already point somewhere unexpected**

Run:

```bash
git ls-remote --heads git@github.com:platinumhamburg/rocksdb.git refs/heads/feature/floor-set-compaction-filter
git ls-remote --heads git@gitlab.alibaba-inc.com:fluss/fluss.git refs/heads/release-0.9-ali-5.0-index-v2
git ls-remote --heads git@gitlab.alibaba-inc.com:fluss/fluss-ali.git refs/heads/release-0.9-ali-5.0-index-v2
```

Expected: missing branches print no ref; an existing ref is acceptable only when its commit equals the local intended tip. A different commit stops the task—do not overwrite it.

### Task 2: Verify, Publish, and Resolve the FloorSet RocksDB JNI Snapshot

**Files:**
- Verify: `utilities/compaction_filters/floor_set_compaction_filter.h`
- Test: `utilities/compaction_filters/floor_set_compaction_filter_test.cc`
- Verify: `java/rocksjni/floor_set_compaction_filter_jni.cc`
- Verify: `java/src/main/java/org/rocksdb/FloorSetCompactionFilter.java`
- Test: `java/src/test/java/org/rocksdb/FloorSetCompactionFilterTest.java`
- Generate: `/Users/wangyang/.cache/rocksdbjni-build/dist/rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar`

**Interfaces:**
- Consumes: remote-checkoutable RocksDB branch at `ac10bdc` and `build-rocksdbjni-hidden` helper.
- Produces: `io.github.platinumhamburg:rocksdbjni-hidden:11.5.0-index-v2-20260804-SNAPSHOT`, independently resolvable from the internal Snapshot repository.

- [ ] **Step 1: Run the C++ FloorSet filter test at the source commit**

Run from `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/rocksdb`:

```bash
./build_tools/version.sh full
make -j4 floor_set_compaction_filter_test
./floor_set_compaction_filter_test
```

Expected: source version is `11.5.0`; binary exits `0`; tests cover floor, explicit set, short values, signed Big-Endian values, and tag-only values.

- [ ] **Step 2: Compile and run the focused Java/JNI test**

Run:

```bash
make -j4 jtest_compile
cd java
java -ea -Xcheck:jni -Djava.library.path=target -cp "target/classes:target/test-classes:test-libs/*:target/*" org.rocksdb.test.RocksJunitRunner org.rocksdb.FloorSetCompactionFilterTest
```

Expected: `FloorSetCompactionFilterTest` passes with no JNI warnings or native crash.

- [ ] **Step 3: Obtain push authorization for the existing three RocksDB commits**

Present this exact scope to the user before the push:

```text
Push feature/floor-set-compaction-filter at ac10bdc to github.com/platinumhamburg/rocksdb.
The branch contains exactly 5b6dd34, a00b8ab, and ac10bdc above origin/main.
No force push and no tag push.
```

Expected: explicit user confirmation. Without it, stop before Step 4.

- [ ] **Step 4: Push and verify the checkoutable feature branch**

Run:

```bash
git push -u origin feature/floor-set-compaction-filter
git ls-remote --heads origin refs/heads/feature/floor-set-compaction-filter
```

Expected: remote branch resolves to the full commit containing `ac10bdc`; `git status --short` remains empty.

- [ ] **Step 5: Create a fresh, preserved checkout from the remote feature branch**

The top-level helper performs repository-wide cleanup that is forbidden in this workspace. Reuse its Linux hidden-export build script against a new read-only checkout instead; do not invoke `build.sh`, `git clean`, or a whole-tree checkout.

Run:

```bash
rocks_build_root=/Users/wangyang/.cache/rocksdbjni-build/index-v2-20260804
rocks_build_src=/Users/wangyang/.cache/rocksdbjni-build/index-v2-20260804/rocksdb-src
rocks_build_out=/Users/wangyang/.cache/rocksdbjni-build/index-v2-20260804/output
test ! -e "$rocks_build_root"
mkdir -p "$rocks_build_out"
git clone --depth 1 --branch feature/floor-set-compaction-filter https://github.com/platinumhamburg/rocksdb.git "$rocks_build_src"
git -C "$rocks_build_src" rev-parse HEAD
git -C "$rocks_build_src" status --short --branch
```

Expected: checkout resolves to the same full hash as remote `feature/floor-set-compaction-filter` and is clean. The fixed build directory must not pre-exist; an existing directory stops the step for review rather than being deleted or overwritten.

- [ ] **Step 6: Build Linux amd64 JNI with hidden symbols and package the fat JAR**

Run:

```bash
rocks_build_src=/Users/wangyang/.cache/rocksdbjni-build/index-v2-20260804/rocksdb-src
rocks_build_out=/Users/wangyang/.cache/rocksdbjni-build/index-v2-20260804/output
rocks_dist=/Users/wangyang/.cache/rocksdbjni-build/dist
rocks_jar=/Users/wangyang/.cache/rocksdbjni-build/dist/rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar
mkdir -p "$rocks_dist"
test ! -e "$rocks_jar"
docker run --rm \
  --name rocksdb_index_v2_linux_amd64 \
  --platform linux/amd64 \
  --attach stdin --attach stdout --attach stderr \
  --volume "$rocks_build_src:/rocksdb-host:ro" \
  --volume "$rocks_build_out:/rocksdb-java-target" \
  --volume /Users/wangyang/.agents/skills/build-rocksdbjni-hidden/docker-build-hidden.sh:/build-helper.sh:ro \
  --env DEBUG_LEVEL=0 \
  --env J=4 \
  --env SKIP_HIDDEN=false \
  --env BASE_NAME=rocksdbhidden \
  evolvedbinary/rocksjava:centos7_x64-be \
  evolvedbinary/rocksjava:centos7_x64-be \
  bash -c 'bash /build-helper.sh && cp -r /rocksdb-local-build/java/target/classes /rocksdb-java-target/classes'
cp "$rocks_build_out/librocksdbjni-linux64.so" "$rocks_build_out/librocksdbhiddenjni-linux64.so"
jar --create --file "$rocks_jar" -C "$rocks_build_out/classes" org
jar --update --file "$rocks_jar" -C "$rocks_build_out" librocksdbhiddenjni-linux64.so
```

Expected: `/Users/wangyang/.cache/rocksdbjni-build/dist/rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar` exists; the helper reports hidden vtable symbols; the preserved source checkout remains clean.

- [ ] **Step 7: Verify JAR entries and Linux exports before publication**

Run:

```bash
rocks_jar=/Users/wangyang/.cache/rocksdbjni-build/dist/rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar
jar tf "$rocks_jar" | rg '^org/rocksdb/FloorSetCompactionFilter.class$'
jar tf "$rocks_jar" | rg '^librocksdbhiddenjni-linux64.so$'
if jar tf "$rocks_jar" | rg '^librocksdbjni-linux64.so$'; then
  exit 1
fi
javap -classpath "$rocks_jar" -c org.rocksdb.NativeLibraryLoader | rg 'rocksdbhidden'
```

Expected: first two commands match exactly once; the third command returns no match; bytecode contains the `rocksdbhidden` loader base name.

Extract and inspect the native file:

```bash
rocks_inspect_dir=$(mktemp -d /tmp/rocksdbjni-index-v2-inspect.XXXXXX)
unzip -j "$rocks_jar" librocksdbhiddenjni-linux64.so -d "$rocks_inspect_dir"
docker run --rm --platform linux/amd64 -v "$rocks_inspect_dir:/inspect:ro" debian:bookworm-slim sh -c 'apt-get update >/dev/null && apt-get install -y binutils >/dev/null && nm -D --defined-only /inspect/librocksdbhiddenjni-linux64.so | grep Java_org_rocksdb | head && ! nm -D --defined-only /inspect/librocksdbhiddenjni-linux64.so | grep -E "ZTVN7rocksdb|ZTIN7rocksdb"'
```

Expected: exported symbols are JNI functions such as `Java_org_rocksdb_*`; no RocksDB C++ vtable/RTTI symbol such as `ZTVN7rocksdb` appears.

- [ ] **Step 8: Deploy the verified file to the internal Maven Snapshot repository**

Run:

```bash
mvn -B deploy:deploy-file \
  -DgroupId=io.github.platinumhamburg \
  -DartifactId=rocksdbjni-hidden \
  -Dversion=11.5.0-index-v2-20260804-SNAPSHOT \
  -Dpackaging=jar \
  -Dfile=/Users/wangyang/.cache/rocksdbjni-build/dist/rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar \
  -DrepositoryId=snapshots \
  -Durl=http://mvnrepo.alibaba-inc.com/mvn/snapshots
```

Expected: Maven reports `BUILD SUCCESS` and a timestamped Snapshot upload. Do not print or copy credentials from Maven settings.

- [ ] **Step 9: Resolve from an empty Maven repository and compare content**

Run:

```bash
rocks_resolve_repo=$(mktemp -d /tmp/rocksdbjni-index-v2-m2.XXXXXX)
mvn -B -U -Dmaven.repo.local="$rocks_resolve_repo" dependency:get -Dartifact=io.github.platinumhamburg:rocksdbjni-hidden:11.5.0-index-v2-20260804-SNAPSHOT
resolved_jar=$(find "$rocks_resolve_repo/io/github/platinumhamburg/rocksdbjni-hidden/11.5.0-index-v2-20260804-SNAPSHOT" -name '*.jar' -type f | head -1)
shasum -a 256 /Users/wangyang/.cache/rocksdbjni-build/dist/rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar "$resolved_jar"
jar tf "$resolved_jar" | rg 'FloorSetCompactionFilter.class|librocksdbhiddenjni-linux64.so'
```

Expected: both SHA-256 values are identical and both required entries are present. If resolution fails, do not start Fluss migration.

### Task 3: Create the Fluss Release Branch and Migrate the Four V2 Commits

**Files:**
- Migrate: exactly the 277 paths in `1ca13b429..5337298d4`
- Modify manually: none in this task

**Interfaces:**
- Consumes: Fluss base `d0b5639` and four ordered V2 commits.
- Produces: local `release-0.9-ali-5.0-index-v2` containing the complete V2 implementation before dependency rewiring.

- [ ] **Step 1: Invoke `superpowers:using-git-worktrees` and validate the existing main worktree**

Use the skill to confirm `/Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree` is the isolated writable Fluss worktree and both backup worktrees remain untouched.

Expected: no new development is placed in `fluss-index-v2` or `index-v2-worktree`.

- [ ] **Step 2: Obtain authorization for the four cherry-pick commits**

Present the exact range:

```text
Create local release-0.9-ali-5.0-index-v2 at d0b5639 and cherry-pick, in order:
9d14e35af, afc558058, 149b0ab47, 5337298d4.
This creates four local commits and does not push them.
```

Expected: explicit user confirmation before any `git cherry-pick` command.

- [ ] **Step 3: Create the target branch from the exact internal release commit**

Run:

```bash
cd /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree
git status --short
git switch -c release-0.9-ali-5.0-index-v2 d0b5639691e638b79ffffd57848313e186686b54
git rev-parse HEAD
```

Expected: clean tree before switch; new branch starts exactly at `d0b5639691e638b79ffffd57848313e186686b54`.

- [ ] **Step 4: Cherry-pick the complete V2 chain in order**

Run one command at a time:

```bash
git cherry-pick 9d14e35af6a7b725976e5e2f639e74d361494007
git cherry-pick afc558058
git cherry-pick 149b0ab47
git cherry-pick 5337298d4
```

Expected: all four commits apply without unmerged files. If any command stops, report the exact unmerged paths before editing; do not skip a commit and do not continue the sequence blindly.

- [ ] **Step 5: Prove the migrated history corresponds to the source chain**

Run:

```bash
git range-diff 1ca13b429c9679807406d8b7b9bdbb537b63070c..5337298d4 d0b5639691e638b79ffffd57848313e186686b54..HEAD
git diff --name-status d0b5639691e638b79ffffd57848313e186686b54..HEAD | wc -l
git log --oneline --reverse d0b5639691e638b79ffffd57848313e186686b54..HEAD
```

Expected: range-diff pairs all four commits, path count is `277`, and no `92b8edc36` or `1ca13b429` commit appears in the target history.

### Task 4: Rewire Fluss to the Published Hidden RocksDB JNI and Run V2 Verification

**Files:**
- Modify: `pom.xml`
- Modify: `fluss-common/pom.xml`
- Test existing: `fluss-common/src/test/java/org/apache/fluss/metadata/PartitionTombstoneTest.java`
- Test existing: `fluss-common/src/test/java/org/apache/fluss/record/ProgressKvRecordBatchTest.java`
- Test existing: `fluss-server/src/test/java/org/apache/fluss/server/index/CompactionFilterITCase.java`
- Test existing: all `fluss-server/src/test/java/org/apache/fluss/server/index/Index*Test.java` and `Index*ITCase.java`
- Test existing: `fluss-client/src/test/java/org/apache/fluss/client/lookup/SecondaryIndexLookuperTest.java`
- Test existing: `fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/source/FlinkSecondaryIndexLookupITCase.java`

**Interfaces:**
- Consumes: migrated V2 source and remotely resolvable RocksDB Snapshot.
- Produces: buildable/tested Fluss release branch that loads `rocksdbhidden` and exposes complete V2 behavior.

- [ ] **Step 1: Verify the clean-repository compile fails with the old dependency**

Run in a Linux amd64 Maven container so only remotely published dependencies are visible:

```bash
cd /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v "$PWD:/workspace" \
  -v /Users/wangyang/.m2/settings.xml:/tmp/index-v2-settings.xml:ro \
  -e MAVEN_CONFIG=/tmp/index-v2-user/.m2 \
  -e MAVEN_OPTS=-Duser.home=/tmp/index-v2-user \
  -w /workspace/index-v2-rebase-squash-worktree \
  maven:3.9.9-eclipse-temurin-11 \
  mvn -B -U -s /tmp/index-v2-settings.xml -Dmaven.repo.local=/tmp/index-v2-red-m2 -pl fluss-server -am -DskipTests compile
```

Expected: FAIL at `FloorSetCompactionFilterFactory` because `com.ververica:frocksdbjni` does not contain `org.rocksdb.FloorSetCompactionFilter`. A different failure, especially repository authentication, must be resolved before editing the POM.

- [ ] **Step 2: Replace the managed dependency in `pom.xml`**

Use `apply_patch` to replace:

```xml
<frocksdb.version>6.20.3-ververica-2.0</frocksdb.version>
```

with:

```xml
<rocksdbjni.hidden.version>11.5.0-index-v2-20260804-SNAPSHOT</rocksdbjni.hidden.version>
```

and replace the managed dependency with:

```xml
<dependency>
    <groupId>io.github.platinumhamburg</groupId>
    <artifactId>rocksdbjni-hidden</artifactId>
    <version>${rocksdbjni.hidden.version}</version>
</dependency>
```

- [ ] **Step 3: Replace the direct dependency in `fluss-common/pom.xml`**

Use `apply_patch` so the RocksDB dependency is exactly:

```xml
<dependency>
    <groupId>io.github.platinumhamburg</groupId>
    <artifactId>rocksdbjni-hidden</artifactId>
</dependency>
```

Expected: `rg -n 'com.ververica|frocksdbjni|frocksdb.version' pom.xml fluss-common/pom.xml` returns no match for the removed dependency.

- [ ] **Step 4: Verify dependency resolution and Java API presence**

Run:

```bash
mvn -B -U -Dmaven.repo.local=../fluss-index-v2/.cache -pl fluss-common dependency:tree -Dincludes=io.github.platinumhamburg:rocksdbjni-hidden
jar tf ../fluss-index-v2/.cache/io/github/platinumhamburg/rocksdbjni-hidden/11.5.0-index-v2-20260804-SNAPSHOT/rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar | rg 'FloorSetCompactionFilter.class|librocksdbhiddenjni-linux64.so'
```

Expected: dependency tree contains the exact Snapshot once; both JAR entries match.

- [ ] **Step 5: Run focused common, server, client, and Flink tests on Linux amd64**

Run from the workspace root:

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v "$PWD:/workspace" \
  -v /Users/wangyang/.m2/settings.xml:/tmp/index-v2-settings.xml:ro \
  -e MAVEN_CONFIG=/tmp/index-v2-user/.m2 \
  -e MAVEN_OPTS=-Duser.home=/tmp/index-v2-user \
  -w /workspace/index-v2-rebase-squash-worktree \
  maven:3.9.9-eclipse-temurin-11 \
  mvn -B -U -s /tmp/index-v2-settings.xml -Dmaven.repo.local=../fluss-index-v2/.cache \
  -pl fluss-common,fluss-server,fluss-client,fluss-flink/fluss-flink-common -am \
  -Dtest=PartitionTombstoneTest,ProgressKvRecordBatchTest,CompactionFilterITCase,IndexPartitionFenceTest,IndexPushModelTest,IndexPushReplicationITCase,IndexPushOrderingITCase,IndexPushFailoverITCase,IndexSourceRemoteRecoveryITCase,IndexTargetRecoveryITCase,SecondaryIndexLookuperTest,FlussTableSecondaryIndexLookuperITCase,FlinkSecondaryIndexLookupITCase \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: all named tests pass, including native compaction, sync/async replication, ordering/failover, remote WAL recovery, target recovery, Java lookup, and Flink lookup.

- [ ] **Step 6: Run the project-standard compile in Linux amd64**

Run from the workspace root:

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v "$PWD:/workspace" \
  -v /Users/wangyang/.m2/settings.xml:/tmp/index-v2-settings.xml:ro \
  -e MAVEN_CONFIG=/tmp/index-v2-user/.m2 \
  -e MAVEN_OPTS=-Duser.home=/tmp/index-v2-user \
  -w /workspace/index-v2-rebase-squash-worktree \
  maven:3.9.9-eclipse-temurin-11 \
  mvn -s /tmp/index-v2-settings.xml clean compile -o -Dmaven.repo.local=../fluss-index-v2/.cache -pl '!fluss-lake/fluss-lake-lance,!fluss-dist'
```

Expected: `BUILD SUCCESS`. If offline mode reports an uncached artifact, warm the cache with this exact online command:

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v "$PWD:/workspace" \
  -v /Users/wangyang/.m2/settings.xml:/tmp/index-v2-settings.xml:ro \
  -e MAVEN_CONFIG=/tmp/index-v2-user/.m2 \
  -e MAVEN_OPTS=-Duser.home=/tmp/index-v2-user \
  -w /workspace/index-v2-rebase-squash-worktree \
  maven:3.9.9-eclipse-temurin-11 \
  mvn -s /tmp/index-v2-settings.xml clean compile -Dmaven.repo.local=../fluss-index-v2/.cache -pl '!fluss-lake/fluss-lake-lance,!fluss-dist'
```

Then rerun the offline compile command shown at the start of Step 6 and require `BUILD SUCCESS`.

- [ ] **Step 7: Run the project-standard full test suite in Linux amd64**

Run:

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v "$PWD:/workspace" \
  -v /Users/wangyang/.m2/settings.xml:/tmp/index-v2-settings.xml:ro \
  -e MAVEN_CONFIG=/tmp/index-v2-user/.m2 \
  -e MAVEN_OPTS=-Duser.home=/tmp/index-v2-user \
  -w /workspace/index-v2-rebase-squash-worktree \
  maven:3.9.9-eclipse-temurin-11 \
  mvn -s /tmp/index-v2-settings.xml test -o -Dmaven.repo.local=../fluss-index-v2/.cache -pl '!fluss-lake/fluss-lake-lance,!fluss-dist'
```

Expected: `BUILD SUCCESS`; no native loader collision, unknown footer, or missing `rocksdbhiddenjni` error. If an uncached test dependency is the only failure, warm the test cache with:

```bash
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v "$PWD:/workspace" \
  -v /Users/wangyang/.m2/settings.xml:/tmp/index-v2-settings.xml:ro \
  -e MAVEN_CONFIG=/tmp/index-v2-user/.m2 \
  -e MAVEN_OPTS=-Duser.home=/tmp/index-v2-user \
  -w /workspace/index-v2-rebase-squash-worktree \
  maven:3.9.9-eclipse-temurin-11 \
  mvn -s /tmp/index-v2-settings.xml test -Dmaven.repo.local=../fluss-index-v2/.cache -pl '!fluss-lake/fluss-lake-lance,!fluss-dist'
```

Then rerun the offline test command shown at the start of Step 7 and require `BUILD SUCCESS`.

- [ ] **Step 8: Audit the only manual Fluss delta**

Run:

```bash
git diff --check
git diff --stat HEAD -- pom.xml fluss-common/pom.xml
git diff HEAD -- pom.xml fluss-common/pom.xml
git status --short
```

Expected: only `pom.xml` and `fluss-common/pom.xml` are uncommitted; root project version remains `0.9-ali-5.0`; the only content change is the Maven coordinate/property replacement.

- [ ] **Step 9: Obtain commit authorization and commit the dependency switch**

Present the two-file diff and the focused/full test results. After explicit confirmation, run:

```bash
git add -- pom.xml fluss-common/pom.xml
git diff --cached --check
git commit -m "build: use index v2 rocksdb jni"
```

Expected: one new commit; clean working tree.

- [ ] **Step 10: Obtain push authorization and push the Fluss target branch**

After explicit confirmation, configure the internal remote only if absent, then run:

```bash
git remote add internal git@gitlab.alibaba-inc.com:fluss/fluss.git
git push -u internal release-0.9-ali-5.0-index-v2
git ls-remote --heads internal refs/heads/release-0.9-ali-5.0-index-v2
```

Expected: remote tip equals local `HEAD`; no force option is used. If `internal` already exists, verify `git remote get-url internal` is the exact GitLab URL and skip `git remote add`.

### Task 5: Create the fluss-ali Release Branch and Expose `fluss-index`

**Files:**
- Modify: `fluss-ali-flink/fluss-ali-vvr-common/src/main/java/org/apache/fluss/flink/catalog/FlussAliCatalogFactory.java`
- Modify: `fluss-ali-flink/fluss-ali-vvr-common/src/test/java/org/apache/fluss/flink/catalog/FlussAliTableFactoryTest.java`

**Interfaces:**
- Consumes: fluss-ali baseline `0b16fc4` and the `fluss-index` behavior represented by historical commit `4651da0`.
- Produces: local fluss-ali `release-0.9-ali-5.0-index-v2` with a dedicated Catalog identifier and no historical DeltaJoin migration.

- [ ] **Step 1: Invoke `superpowers:using-git-worktrees` and create a dedicated fluss-ali worktree**

After the skill verifies the parent repository is clean, run:

```bash
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali worktree add -b release-0.9-ali-5.0-index-v2 /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali-index-v2-release-worktree 0b16fc415663f2bf6afe55c0668469e630fc0950
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali-index-v2-release-worktree rev-parse HEAD
```

Expected: target worktree starts exactly at `0b16fc4`; the existing `fluss-ali` checkout remains on its original branch and unchanged.

- [ ] **Step 2: Install the Fluss target artifacts under the image build version**

In the Fluss target worktree, run the same versioning and module set used by pipeline `142673`:

```bash
mvn -B org.codehaus.mojo:versions-maven-plugin:2.15.0:set -DnewVersion=0.9-ali-5.0-index-v2
mvn -B -Dmaven.repo.local=../fluss-index-v2/.cache -pl fluss-common,fluss-dist,fluss-filesystems,fluss-lake,fluss-metrics,fluss-protogen,fluss-rpc,fluss-server -am clean install -DskipTests -Dmaven.checkstyle.skip=true -Dautoconfig.skip
mvn -B org.codehaus.mojo:versions-maven-plugin:2.15.0:revert
git status --short
```

Expected: install succeeds; `versions:revert` leaves the Fluss worktree clean. If it leaves a tracked POM diff, stop and review exact paths instead of discarding them in bulk.

- [ ] **Step 3: Set the fluss-ali worktree to the image build version temporarily**

Run from the fluss-ali target worktree:

```bash
mvn -B org.codehaus.mojo:versions-maven-plugin:2.15.0:set -DnewVersion=0.9-ali-5.0-index-v2
```

Expected: reactor POM versions now resolve the locally installed Fluss target artifacts; `.versionsBackup` files preserve the baseline versions for Step 8.

- [ ] **Step 4: Add the focused identifier test first**

Use `apply_patch` to add this method to `FlussAliTableFactoryTest`:

```java
@Test
void testCatalogFactoryUsesDedicatedIndexIdentifier() {
    assertThat(new FlussAliCatalogFactory().factoryIdentifier()).isEqualTo("fluss-index");
}
```

- [ ] **Step 5: Run the test and verify the expected failure**

Run from the fluss-ali target worktree:

```bash
mvn -B -Dmaven.repo.local=../fluss-index-v2/.cache -pl fluss-ali-flink/fluss-ali-vvr-common -am -Dtest=FlussAliTableFactoryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the baseline factory returns `fluss`, proving the new assertion exercises the intended behavior rather than passing accidentally.

- [ ] **Step 6: Apply the historical behavior without importing its other branch history**

Use `apply_patch` to change exactly:

```java
public static final String IDENTIFIER = "fluss";
```

to:

```java
public static final String IDENTIFIER = "fluss-index";
```

Then verify equivalence:

```bash
git show 4651da0 -- fluss-ali-flink/fluss-ali-vvr-common/src/main/java/org/apache/fluss/flink/catalog/FlussAliCatalogFactory.java
git diff -- fluss-ali-flink/fluss-ali-vvr-common/src/main/java/org/apache/fluss/flink/catalog/FlussAliCatalogFactory.java
```

Expected: the production-code hunk is semantically identical to `4651da0`; no Catalog injection or DeltaJoin property appears.

- [ ] **Step 7: Re-run the focused test and package the pipeline module set**

Run:

```bash
mvn -B -Dmaven.repo.local=../fluss-index-v2/.cache -pl fluss-ali-flink/fluss-ali-vvr-common -am -Dtest=FlussAliTableFactoryTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -B -Dmaven.repo.local=../fluss-index-v2/.cache -pl fluss-ali-alake/fluss-ali-alake-lake/fluss-ali-alake-lake-paimon,fluss-ali-auth,fluss-ali-dist,fluss-ali-filesystems -am clean package -DskipTests -Dmaven.checkstyle.skip=true -Dautoconfig.skip -DuseCache=true -P alake
```

Expected: focused test passes and package succeeds with the locally installed `0.9-ali-5.0-index-v2` Fluss artifacts.

- [ ] **Step 8: Revert temporary versions and verify the final fluss-ali scope**

Run:

```bash
mvn -B org.codehaus.mojo:versions-maven-plugin:2.15.0:revert
rg -n 'FlussAliCatalogFactory' fluss-ali-flink/fluss-ali-vvr-11/src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory
if git diff -- fluss-ali-flink/fluss-ali-vvr-common/src/test | rg 'table.secondary-index.columns|delta.join|DeltaJoin'; then
  exit 1
fi
git diff --name-only
git status --short
```

Expected: service descriptor still registers `FlussAliCatalogFactory`; the legacy-test scan returns no match; only the factory and its focused test are uncommitted, with no POM or `.versionsBackup` path left.

- [ ] **Step 9: Obtain commit authorization and commit the two-file fluss-ali change**

After explicit confirmation, run:

```bash
git add -- fluss-ali-flink/fluss-ali-vvr-common/src/main/java/org/apache/fluss/flink/catalog/FlussAliCatalogFactory.java fluss-ali-flink/fluss-ali-vvr-common/src/test/java/org/apache/fluss/flink/catalog/FlussAliTableFactoryTest.java
git diff --cached --check
git commit -m "feat: expose secondary index catalog"
```

Expected: one commit above `0b16fc4`; clean worktree.

- [ ] **Step 10: Obtain push authorization and push the fluss-ali target branch**

After explicit confirmation, run:

```bash
git push -u origin release-0.9-ali-5.0-index-v2
git ls-remote --heads origin refs/heads/release-0.9-ali-5.0-index-v2
```

Expected: remote tip equals local `HEAD`; no force option is used.

### Task 6: Audit Both Release Branches Before CI

**Files:**
- Read: all changed files in both target branches
- Modify: none

**Interfaces:**
- Consumes: pushed Fluss and fluss-ali target branches plus published JNI Snapshot.
- Produces: a release manifest of exact commits, dependency coordinate, and remotely visible branch tips suitable for CI checkout.

- [ ] **Step 1: Verify the Fluss branch ancestry and manual delta**

Run:

```bash
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree merge-base --is-ancestor d0b5639691e638b79ffffd57848313e186686b54 HEAD
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree log --oneline --reverse d0b5639691e638b79ffffd57848313e186686b54..HEAD
rg -n 'rocksdbjni.hidden.version|rocksdbjni-hidden' /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree/pom.xml /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree/fluss-common/pom.xml
```

Expected: four migrated V2 commits plus one dependency commit; exact Snapshot coordinate appears in dependency management and `fluss-common`.

- [ ] **Step 2: Verify the fluss-ali branch ancestry and scope**

Run:

```bash
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali-index-v2-release-worktree merge-base --is-ancestor 0b16fc415663f2bf6afe55c0668469e630fc0950 HEAD
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali-index-v2-release-worktree diff --name-status 0b16fc415663f2bf6afe55c0668469e630fc0950..HEAD
rg -n 'IDENTIFIER = "fluss-index"' /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali-index-v2-release-worktree/fluss-ali-flink/fluss-ali-vvr-common/src/main/java/org/apache/fluss/flink/catalog/FlussAliCatalogFactory.java
```

Expected: exactly two changed paths—the factory and focused test—and identifier is `fluss-index`.

- [ ] **Step 3: Verify both remote tips and the remote Maven artifact immediately before CI**

Run:

```bash
git ls-remote --heads git@gitlab.alibaba-inc.com:fluss/fluss.git refs/heads/release-0.9-ali-5.0-index-v2
git ls-remote --heads git@gitlab.alibaba-inc.com:fluss/fluss-ali.git refs/heads/release-0.9-ali-5.0-index-v2
mvn -B -U dependency:get -Dartifact=io.github.platinumhamburg:rocksdbjni-hidden:11.5.0-index-v2-20260804-SNAPSHOT
```

Expected: Git refs equal the audited local tips and Maven resolution succeeds.

### Task 7: Reuse Aone Pipeline 142673 to Build the Independent Image

**Files:**
- Read only: fluss-ali `.aoneci/build-image-inner-v2.yaml`
- Modify: none

**Interfaces:**
- Consumes: two remote target branches, Maven Snapshot, and the four exact pipeline parameters.
- Produces: successful Aone run and immutable image digest for the standalone index-v2 image.

- [ ] **Step 1: Invoke the `aone-ci` skill and verify pipeline visibility**

Run:

```bash
opencli aone-ci pipelines --project fluss-ali --keyword 构建弹内镜像V2
opencli aone-ci runs --project fluss-ali --pipeline 142673 --limit 5
```

Expected: pipeline `142673` is visible and baseline run `57762814` remains `SUCCESS`.

- [ ] **Step 2: Check the normal image tag before triggering**

Run:

```bash
image_ref=turbo-x-registry.cn-zhangjiakou.cr.aliyuncs.com/fluss/fluss:release-0.9-ali-5.0-index-v2
manifest_output=$(docker manifest inspect "$image_ref" 2>&1)
manifest_rc=$?
if [[ "$manifest_rc" -eq 0 ]]; then
  image_tag_state=exists
elif echo "$manifest_output" | rg -i 'manifest unknown|no such manifest|not found'; then
  image_tag_state=absent
else
  echo "$manifest_output"
  exit "$manifest_rc"
fi
echo "$image_tag_state"
```

Expected normal path: manifest lookup reports that the tag is absent. If it succeeds, do not overwrite the tag; execute Step 3. If it is absent, skip Step 3 and continue to Step 4.

- [ ] **Step 3: Apply the fixed `-2` fallback when the normal tag already exists**

First obtain user confirmation to create and push one additional Fluss alias branch pointing at the already-audited target commit. Then run:

```bash
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree branch release-0.9-ali-5.0-index-v2-2 release-0.9-ali-5.0-index-v2
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree push -u internal release-0.9-ali-5.0-index-v2-2
fallback_output=$(docker manifest inspect turbo-x-registry.cn-zhangjiakou.cr.aliyuncs.com/fluss/fluss:release-0.9-ali-5.0-index-v2-2 2>&1)
fallback_rc=$?
if [[ "$fallback_rc" -eq 0 ]]; then
  exit 1
elif ! echo "$fallback_output" | rg -i 'manifest unknown|no such manifest|not found'; then
  echo "$fallback_output"
  exit "$fallback_rc"
fi
```

Expected: alias branch points at the same Fluss commit; fallback tag is absent. For all remaining commands set:

```bash
ci_fluss_branch=release-0.9-ali-5.0-index-v2-2
ci_version=0.9-ali-5.0-index-v2-2
```

Normal path values are:

```bash
ci_fluss_branch=release-0.9-ali-5.0-index-v2
ci_version=0.9-ali-5.0-index-v2
```

- [ ] **Step 4: Dry-run the exact pipeline parameters**

Run:

```bash
opencli aone-ci trigger --project fluss-ali --pipeline 142673 --branch release-0.9-ali-5.0-index-v2 --dry-run \
  --param "VERSION=${ci_version},FLUSS_ALI_GIT_BRANCH=release-0.9-ali-5.0-index-v2,FLUSS_GIT_URL=git@gitlab.alibaba-inc.com:fluss/fluss.git,FLUSS_GIT_BRANCH=${ci_fluss_branch}"
```

Expected: dry-run identifies the fluss-ali target branch, internal Fluss GitLab URL, selected Fluss branch, and selected Maven version; no pipeline run is created.

- [ ] **Step 5: Trigger one formal pipeline run**

Run:

```bash
trigger_output=$(opencli aone-ci trigger --project fluss-ali --pipeline 142673 --branch release-0.9-ali-5.0-index-v2 \
  --param "VERSION=${ci_version},FLUSS_ALI_GIT_BRANCH=release-0.9-ali-5.0-index-v2,FLUSS_GIT_URL=git@gitlab.alibaba-inc.com:fluss/fluss.git,FLUSS_GIT_BRANCH=${ci_fluss_branch}")
echo "$trigger_output"
run_id=$(echo "$trigger_output" | jq -r '.runId // .run.id // .id')
test -n "$run_id"
```

Expected: exactly one run is created and `run_id` is a numeric ID.

- [ ] **Step 6: Monitor every job until a terminal state**

Run at intervals no longer than 30 seconds, sharing a user-facing progress update at least once per minute:

```bash
opencli aone-ci status --project fluss-ali --run-id "$run_id" --jobs
```

Expected success path: run and all jobs reach `SUCCESS`.

- [ ] **Step 7: On failure, collect every failed/cancelled job before deciding next action**

For every `taskNo` returned by status, run:

```bash
opencli aone-ci log --project fluss-ali --run-id "$run_id" --task-no "$task_no" --failed --tail 300
```

Expected: identify the first real `BUILD FAILURE`, compilation error, dependency-resolution error, or image-tag conflict. Treat fast-fail/cancelled siblings as secondary. Do not rerun automatically.

- [ ] **Step 8: Resolve and record the immutable image digest after success**

Run with the selected branch tag:

```bash
image_ref="turbo-x-registry.cn-zhangjiakou.cr.aliyuncs.com/fluss/fluss:${ci_fluss_branch}"
docker buildx imagetools inspect "$image_ref"
```

Expected: registry reports a Linux amd64 manifest and an immutable `sha256:` digest.

- [ ] **Step 9: Verify the image contains the expected Fluss and JNI artifacts**

Run:

```bash
docker run --rm --platform linux/amd64 --entrypoint /bin/sh "$image_ref" -c 'find / -type f \( -name "*0.9-ali-5.0-index-v2*.jar" -o -name "rocksdbjni-hidden-11.5.0-index-v2-20260804-SNAPSHOT.jar" \) -print 2>/dev/null'
```

Expected: output contains Fluss artifacts using the selected `ci_version` and the exact `rocksdbjni-hidden` Snapshot JAR. The command must not start or register a Fluss cluster.

### Task 8: Final Verification and Handoff Without Deployment

**Files:**
- Modify: none

**Interfaces:**
- Consumes: successful tests, remote refs, Maven checksum, Aone run, and image digest.
- Produces: evidence-backed release handoff and explicit rollback boundary.

- [ ] **Step 1: Run final clean-state and remote-tip checks**

Run:

```bash
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/index-v2-rebase-squash-worktree status --short --branch
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/fluss-ali-index-v2-release-worktree status --short --branch
git -C /Users/wangyang/Desktop/Projects/Activate/index-rebase-workspace/rocksdb status --short --branch
git ls-remote --heads git@github.com:platinumhamburg/rocksdb.git refs/heads/feature/floor-set-compaction-filter
git ls-remote --heads git@gitlab.alibaba-inc.com:fluss/fluss.git "refs/heads/${ci_fluss_branch}"
git ls-remote --heads git@gitlab.alibaba-inc.com:fluss/fluss-ali.git refs/heads/release-0.9-ali-5.0-index-v2
```

Expected: all worktrees are clean and every remote ref equals the tested local commit.

- [ ] **Step 2: Present the release evidence**

The final handoff must include actual values collected during execution:

- RocksDB branch full commit and the three included commits.
- Published Maven coordinate and SHA-256.
- Fluss target branch full commit and its five-commit delta from `d0b5639`.
- fluss-ali target branch full commit and its two changed paths.
- Focused test results, standard compile result, and standard full-test result.
- Aone pipeline ID `142673`, run ID, run URL, and `SUCCESS` state.
- Final image URL and immutable digest.
- Explicit statement that no deployment occurred.
- Rollback boundary: safe to use the baseline image before creating an Index Table; after new cumulative-progress KV/WAL/WriterState data exists, downgrade is unsupported.
- Future deployment order is outside this plan; if separately authorized later, upgrade all TabletServers that may host Index Buckets first, then Coordinator, then Java clients and Flink connectors.

- [ ] **Step 3: Do not perform deployment or cleanup that destroys evidence**

Expected: leave source branches, release worktrees, Maven resolution directory, CI logs, and image digest available for review. Do not alter clusters, delete worktrees, remove branches, or overwrite image tags as part of this plan.
