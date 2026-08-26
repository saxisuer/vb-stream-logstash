# vb-stream-logstash

适配 PostgreSQL 逻辑解码 **stream 模式**的 CDC 采集器：基于 pgjdbc `ReplicationConnection` 直连复制流，自研 pgoutput 协议解码器，实时解析普通事务、流式大事务（`streaming=parallel`）、两阶段提交（`two_phase`）与 Truncate。

- 坐标：`org.vastdata:vb-stream-logstash:1.0-SNAPSHOT`（Vastbase 生态）
- 工具链：Java 17 + Maven；日志 slf4j + logback
- 状态：里程碑 1 完成——协议层 19 种消息全量解析、复制会话、可运行 `Main`，52 个测试（单元 + Testcontainers 集成）全绿

## PostgreSQL 18 前置要求

| 项 | 要求 | 说明 |
|---|---|---|
| 版本 | **PG 14+**，按 PG 18 开发验证 | stream 模式需 PG 14 复制槽 `streaming` 选项；`proto_version=4` |
| `wal_level` | `logical` | 逻辑解码必需，默认 `replica` 不行；改后需重启 |
| `max_replication_slots` | ≥ 1 | 每个采集实例占用 1 个逻辑槽 |
| `max_wal_senders` | ≥ 1 | 每条复制连接占用 1 个 walsender |
| `max_prepared_transactions` | > 0（仅 `twoPhase=true` 需要） | **默认 0**，此时 `PREPARE TRANSACTION` 直接报错；槽开启 two_phase 后流里才会出现两阶段消息 |
| `logical_decoding_work_mem` | 按需调低 | 流式触发阈值，**默认 64MB**——大事务几乎不会被流式发送；测试/演示建议 `64kB` |
| publication | 表必须加入 publication | pgoutput 协议硬性要求传 `publication_names`；不在 publication 中的表不产生事件（`FOR ALL TABLES` 或逐表添加） |
| 复制连接 | URL 带 `?replication=database` | 且 pgjdbc 要求同时 `assumeMinServerVersion>=9.4`，否则 `START_REPLICATION` 被按普通 SQL 解析报语法错 |
| 复制槽 | `two_phase` 属性随建槽开启 | 建槽后不可更改；槽持久存在，客户端失联期间 WAL 按槽保留（注意 `max_slot_wal_keep_size` 防磁盘膨胀） |

一键满足上述全部要求的本地环境（PG 18 + 合规 conf + 测试表 + publication）：

```bash
cd src/docker && docker compose up -d     # localhost:55432，postgres/postgres
```

## 运行 Main

`Main` 是里程碑 1 的端到端入口：建槽 → 开流 → 解码打印到控制台，`Ctrl+C` 优雅退出。

```bash
cd src/docker && docker compose up -d && cd ../..    # 起本地 PG（已起可跳过）
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes:$(cat target/cp.txt)" org.vastdata.vbstream.Main
```

### 配置（系统属性，均有默认值对准 src/docker 环境）

| 属性 | 默认值 | 说明 |
|---|---|---|
| `-Dvb.pg.host` / `-Dvb.pg.port` | `localhost` / `55432` | |
| `-Dvb.pg.database` | `postgres` | |
| `-Dvb.pg.user` / `-Dvb.pg.password` | `postgres` / `postgres` | |
| `-Dvb.pg.slot` | `vb_cdc_slot` | 复制槽名；已存在则复用（two_phase 属性需匹配） |
| `-Dvb.pg.publication` | `vb_pub` | pgoutput 必填参数 |
| `-Dvb.pg.protoVersion` | `4` | |
| `-Dvb.pg.streaming` | `parallel` | `on` / `parallel` / `off` |
| `-Dvb.pg.twoPhase` | `true` | 槽与流的 two_phase 开关 |
| `-Dvb.pg.feedbackSeconds` | `10` | LSN 确认位点回传周期 |

### 输出

- CDC 数据：logger `org.vastdata.vbstream.cdc`，每消息一行 INFO（可独立调级/重定向，与诊断日志区分流）
- 诊断日志：会话生命周期 INFO（连接/建槽/开流/关闭）；解析层逐消息 **DEBUG 默认关闭**，排障时在 `src/main/resources/logback.xml` 加 `<logger name="org.vastdata.vbstream.protocol" level="DEBUG"/>`

```
2026-08-27 02:40:04.661 [main] INFO  o.v.v.r.PgReplicationSession - 复制流已启动: 槽=vb_cdc_slot ...
2026-08-27 02:40:07.060 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc - BEGIN-PREPARE     gid=lg1 xid=769
2026-08-27 02:40:07.067 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc - INSERT            public.t_stream_test [id=1404, payload=logback-smoke, ...]
2026-08-27 02:40:07.068 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc - PREPARE           gid=lg1 xid=769
```

四种事务场景的输出标记：`BEGIN/COMMIT`（普通）、`STREAM-START/STREAM-COMMIT`（流式大事务）、`BEGIN-PREPARE/COMMIT-PREPARED`（两阶段）、`STREAM-PREPARE`（流式 2PC）。

### 注意事项

- **流式触发与写入形态有关**：walsender 已追平时，单语句 `INSERT..SELECT` 批量写入的大事务不触发流式（整段于提交后一次性回放）；需要流式场景请事务内分批/跨秒写入，或调低 `logical_decoding_work_mem`
- **断线续传**：进程退出后槽保留，重启从最后确认的 LSN 续传；确认周期即 `feedbackSeconds`
- 手工清理槽：`SELECT pg_drop_replication_slot('vb_cdc_slot')`（先 `pg_terminate_backend(active_pid)` 若仍活跃）

## 测试

```bash
mvn test                # 全部：协议单元测试 + Testcontainers 集成测试
mvn test -Dtest=StreamedTransactionTest    # 单类
```

集成测试（`org.vastdata.vbstream.it`）经 Testcontainers 自动起 postgres:18 容器（`logical_decoding_work_mem=64kB`），需本机 Docker。

## 路线

- 里程碑 1（完成）：pgoutput 解码器 + 复制会话 + Main/ConsoleListener
- 里程碑 2（计划）：Chronicle Queue 落盘（实现 `PgOutputListener` 即可接入）、与 Logstash 集成
