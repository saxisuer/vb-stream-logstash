# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

vb-stream-logstash 是一个**全新（greenfield）项目**，目标：适配 PostgreSQL 最新的逻辑解码（logical decoding）**stream 模式**，通过 pgjdbc 的 `ReplicationConnection` / `PGReplicationStream` API 实时获取 CDC 数据。

- 坐标：`org.vastdata:vb-stream-logstash:1.0-SNAPSHOT`（Vastbase 生态；artifactId 暗示最终会以某种形式与 Logstash 集成，集成方式尚未确定）
- 工具链：Java 17 + Maven
- 当前状态：**里程碑 1 已完成**（pgoutput 流式解码器：19 种协议消息解析 + 复制会话 + `Main`/`ConsoleListener` + 4 组 Testcontainers 集成用例，`mvn test` 全绿）。核心依赖（版本以 pom 的 `<properties>` 为准）：
    - `org.postgresql:postgresql`（pgjdbc，含逻辑复制 API）
    - `net.openhft:chronicle-queue`（持久化低延迟队列；会传递引入 chronicle-core/bytes/wire/threads 及 `slf4j-api`，注意日志实现尚未引入）

## 常用命令

```bash
mvn clean package                    # 构建
mvn compile                          # 仅编译
mvn test                             # 运行全部测试
mvn test -Dtest=ClassName            # 运行单个测试类
mvn test -Dtest=ClassName#method     # 运行单个测试方法
mvn dependency:tree                  # 查看依赖树
```

注意：测试基于 JUnit 6（JUnit Jupiter，要求 Java 17+）+ Surefire，已可在 `src/test/java` 下直接编写测试。涉及 PG 复制的集成测试可用本地 Docker 起 PostgreSQL 容器。

## 运行 Main（里程碑 1）

```bash
cd src/docker && docker compose up -d && cd ../..     # 起本地 PG
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes:$(cat target/cp.txt)" org.vastdata.vbstream.Main
# 可选覆盖：-Dvb.pg.slot=... -Dvb.pg.publication=... -Dvb.pg.streaming=on|parallel|off
```

- 源码结构：`org.vastdata.vbstream.protocol`（协议解析，纯函数）、`org.vastdata.vbstream.replication`（会话）、`Main`/`ConsoleListener`
- 集成测试（`org.vastdata.vbstream.it`）经 Testcontainers 自动起 postgres:18 容器，需本机 Docker；`mvn test` 单命令跑全部
- src/docker 的 postgresql.conf 已含冒烟所需 `max_prepared_transactions=16` 与 `logical_decoding_work_mem=64kB`（改 conf 后 `docker compose restart postgres`）。注意：walsender 已追平时，单语句 `INSERT..SELECT` 批量写入的大事务不触发流式（整段于提交后回放）；构造流式场景需事务内分批/跨秒写入

## 领域要点（实现时的关键约束）

- 目标是 stream 模式（**流式发送进行中的大事务**），而不是等事务提交后整体回放的传统模式。需要 PG 14+（复制槽 `streaming` 选项），pgoutput 插件需 `proto_version >= 2` 才会收到流式消息。
- pgjdbc 复制 API 的入口链路：JDBC URL 带 `replication=database` 参数 → `PGConnection.getReplicationAPI()` → `ReplicationConnection.createReplicationSlot().logical()...` 建槽 → `createReplicationStream().withSlotOption(...)` 建流 → 循环 `PGReplicationStream.readPending()/read()`，并周期性 `setAppliedLSN()/setFlushedLSN()/forceStatusUpdate()` 回传确认位点。
- 输出插件与 publication 的关系（已核实 PG 18 官方文档）：复制槽与 publication 相互独立，publication 只是 `START_REPLICATION` 的过滤参数。**pgoutput 必须传 `publication_names`（协议硬性要求，至少一个）**；`test_decoding` 无需 publication 且支持流式（stream-changes），定位是测试/示例，适合冒烟联调；wal2json 等第三方插件也免 publication 但需在 PG 侧安装。计划：联调用 test_decoding，生产用 pgoutput + `FOR ALL TABLES` publication。
- 具体细节（快照导出、错误恢复/断线重连、与 Logstash 的集成方式等）尚未确定，涉及这类决策应先与用户确认，不要自行臆断。

## 测试用 PostgreSQL（Docker）

环境定义在 `src/docker/`，已实测可用（PG 18，逻辑解码已开启）：

```bash
cd src/docker
docker compose up -d             # 启动（首次自动建库并执行 initdb.d/）
docker compose restart postgres  # 改 postgresql.conf 后重启生效
docker compose down              # 停止，数据保留
```

- 连接：`jdbc:postgresql://localhost:55432/postgres`，用户/密码 `postgres`/`postgres`；复制连接加 `?replication=database`
- `wal_level=logical` 已配置（`src/docker/postgresql.conf`，改后 restart 即可）；`max_slot_wal_keep_size=2GB` 防止复制槽拖垮磁盘
- 数据持久化在 `src/docker/pgdata/`（已 gitignore）。**PG 18+ 镜像约定**：挂载点是 `/var/lib/postgresql`，真实 PGDATA 是版本化子目录 `/var/lib/postgresql/18/docker`，升级镜像大版本时 postgresql.conf 中的路径需同步改
- 首次初始化会执行 `initdb.d/`：追加 pg_hba 的 replication 放行行（宿主机连复制流必需）、建测试表 `t_stream_test` 和 publication `vb_pub`（pgoutput 逻辑解码要求表在 publication 中）

## 环境备注

- macOS 环境；git 仓库已初始化并推送 GitHub（origin/main）。**跨多台电脑开发，每个任务完成后必须 commit + push。**
- 本地装有 Docker，可用于运行 PostgreSQL 容器做逻辑复制的集成测试。
- IDE 为 IntelliJ IDEA，已通过 IDEA MCP（见 `.mcp.json`）接入本会话，可优先使用其构建/检查类工具。
