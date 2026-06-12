# 孤儿清理作业日志接入 SLS 配置指南

## 1. 依赖 JAR

需要将以下依赖上传到 VVP "文件管理"（Additional Dependencies）：

```
com.aliyun.openservices:aliyun-log-log4j2-appender:0.1.12
com.google.protobuf:protobuf-java:2.5.0
```

下载命令（本地 Maven 缓存或远程仓库）：

```bash
mvn dependency:copy \
  -Dartifact=com.aliyun.openservices:aliyun-log-log4j2-appender:0.1.12 \
  -DoutputDirectory=./sls-jars

mvn dependency:copy \
  -Dartifact=com.google.protobuf:protobuf-java:2.5.0 \
  -DoutputDirectory=./sls-jars
```

> 注：`aliyun-log-log4j2-appender:0.1.12` 内部已打包了 `aliyun-log-producer` 和 `aliyun-log` 依赖（shade），
> 仅需额外补充 `protobuf-java`（Flink 运行时可能已包含，可先不传试一下）。

## 2. VVP 日志配置（log4j2.properties 追加内容）

在 VVP 批作业 → 作业配置 → 日志配置 中，追加以下内容：

```properties
# ============================================================
# SLS Appender - 将作业日志投递到阿里云日志服务
# ============================================================
appender.sls.type = Loghub
appender.sls.name = SLSAppender
appender.sls.project = <SLS_PROJECT>
appender.sls.logStore = <SLS_LOGSTORE>
appender.sls.endpoint = <SLS_ENDPOINT>
appender.sls.accessKeyId = <ACCESS_KEY_ID>
appender.sls.accessKeySecret = <ACCESS_KEY_SECRET>
appender.sls.topic = orphan-files-clean
appender.sls.source = vvp-batch
appender.sls.totalSizeInBytes = 104857600
appender.sls.maxBlockMs = 0
appender.sls.ioThreadCount = 8
appender.sls.batchSizeThresholdInBytes = 524288
appender.sls.batchCountThreshold = 4096
appender.sls.lingerMs = 2000
appender.sls.retries = 10
appender.sls.baseRetryBackoffMs = 100
appender.sls.maxRetryBackoffMs = 50000
appender.sls.timeFormat = yyyy-MM-dd'T'HH:mm:ssZ
appender.sls.timeZone = Asia/Shanghai
appender.sls.ignoreExceptions = true

# 将 SLS Appender 加入 rootLogger（全量日志投递）
rootLogger.appenderRef.sls.ref = SLSAppender
```

### 可选：仅投递 orphan 清理相关日志（推荐）

如果不希望投递全量 Flink 框架日志，替换 `rootLogger.appenderRef.sls.ref` 为独立 logger：

```properties
# 仅将 orphan 清理模块的日志发送到 SLS
logger.orphan.name = org.apache.fluss.flink.action.orphan
logger.orphan.level = INFO
logger.orphan.appenderRef.sls.ref = SLSAppender
logger.orphan.additivity = true
```

## 3. 占位符替换

| 占位符 | 含义 | 示例 |
|--------|------|------|
| `<SLS_PROJECT>` | SLS 项目名 | `fluss-log` |
| `<SLS_LOGSTORE>` | 目标 logstore | `orphan-clean-log` |
| `<SLS_ENDPOINT>` | SLS 服务端点 | `cn-hangzhou-intranet.log.aliyuncs.com` |
| `<ACCESS_KEY_ID>` | RAM AccessKey ID | `LTAI5t...` |
| `<ACCESS_KEY_SECRET>` | RAM AccessKey Secret | `vVyc...` |

> 建议使用内网 endpoint（`-intranet`）以避免公网流量费用。

## 4. 验证步骤

1. 以 `--dry-run` 模式运行一次批作业
2. 在 SLS 控制台查询：`topic: orphan-files-clean`
3. 确认审计摘要、扫描进度等关键日志均已投递
4. 检查 SLS 中日志字段：`level`、`message`、`location`、`thread`、`time`

## 5. 故障排查

如果 SLS 无日志写入：
- 检查 VVP JM stdout 中是否有 `StatusLogger` 打印的 SLS appender 错误
- 确认 endpoint 可达（VVP 网络环境是否允许访问 SLS 内网端点）
- 确认 AccessKey 有 `log:PostLogStoreLogs` 权限
- 确认 `protobuf-java` 版本不与 Flink 内置版本冲突（如有冲突可移除额外上传的 protobuf jar）
