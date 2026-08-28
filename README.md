# vb-stream-logstash

适配 PostgreSQL 逻辑解码 **stream 模式**的 CDC 采集器：基于 pgjdbc `ReplicationConnection` 直连复制流，自研 pgoutput 协议解码器，实时解析普通事务、流式大事务（`streaming=parallel`）、两阶段提交（`two_phase`）与 Truncate，并把原始字节流组装为**原子事务块**输出（组装缓冲越限自动溢写 Chronicle Queue，组装期内存有界）。

- 坐标：`org.vastdata:vb-stream-logstash:1.0-SNAPSHOT`（Vastbase 生态）
- 工具链：Java 17 + Maven；日志 slf4j + logback
- 状态：里程碑 1.6 完成——协议层 19 种消息全量解析、复制会话、事务组装器（MEMORY/SPILLED 混合缓冲 + Chronicle Queue 溢写 + Relation 版本日志（DDL 后旧行按变更时刻表结构渲染）），151 个测试全绿（单元 + Testcontainers 集成），JMH 基线在档（`docs/benchmarks-baseline.md`）

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

`Main` 是端到端入口：建槽 → 开流 → 原始字节流经 `TransactionAssembler` 组装为**原子事务块**打印到控制台，`Ctrl+C` 优雅退出。

```bash
cd src/docker && docker compose up -d && cd ../..    # 起本地 PG（已起可跳过）
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED \
     --add-opens java.base/sun.nio.fs=ALL-UNNAMED \
     --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp "target/classes:$(cat target/cp.txt)" org.vastdata.vbstream.Main
```

`--add-opens` 清单必带：组装缓冲越过溢写阈值时建 Chronicle Queue，其 mmap 在 Java 17 需开放内部包。

> 注：命令为 bash 形态；Windows 下 classpath 分隔符是 `;` 而非 `:`

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

### 溢写配置（`vb.spill.*`，`SpillConfig`）

| 属性 | 默认 | 说明 |
|---|---|---|
| `-Dvb.spill.thresholdBytes` | `67108864`（64MiB） | 全局内存桶字节和阈值，越限即整体转储溢写池；**≤0 = 禁用溢写**（纯内存逃生门） |
| `-Dvb.spill.dir` | `spill-queue` | 溢写队列目录（相对工作目录）；**重启自动清空属预期**——真源是复制槽，PG 从确认位点重发未完事务 |
| `-Dvb.spill.rollCycle` | `MINUTELY` | Chronicle Queue 滚动周期（`LegacyRollCycles` 枚举名，大小写宽容） |

### 输出

- **事务块**（`TXN-BEGIN`/`TXN-END` 头尾 + 逐变更行）与**事务生命周期控制消息**（流式 Stream-Start/Stop/Commit/Abort/Prepare + 两阶段信号，共 9 种）：走 CDC 专用 logger `org.vastdata.vbstream.cdc`，INFO——任何事务形态（含回滚、无数据消息的事务）在 INFO 级至少留一行痕迹
- 行级数据与元数据的逐消息细节：同 logger **DEBUG 默认关闭**（大事务防刷屏），排障时在 `src/main/resources/logback.xml` 加 `<logger name="org.vastdata.vbstream.cdc" level="DEBUG"/>`
- 诊断日志：会话生命周期 INFO（连接/建槽/开流/关闭）

```
2026-08-27 02:40:04.661 [main] INFO  o.v.v.r.PgReplicationSession - 复制流已启动: 槽=vb_cdc_slot ...
2026-08-27 02:40:07.060 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc - STREAM-START      xid=769 firstSegment=true
2026-08-27 02:40:07.064 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc - STREAM-STOP
2026-08-27 02:40:07.065 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc - STREAM-COMMIT     xid=769 commitLsn=0x3a21c60
2026-08-27 02:40:07.068 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc - TXN-BEGIN xid=769 kind=STREAMED gid=null commitLsn=0x3a21c60 commitTs=2026-08-27T02:40:07Z changes=3
2026-08-27 02:40:07.068 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc -   [1] INSERT public.t_stream_test BEFORE=- AFTER=[id=1404, payload=logback-smoke-1]
2026-08-27 02:40:07.068 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc -   [2] INSERT public.t_stream_test BEFORE=- AFTER=[id=1405, payload=logback-smoke-2]
2026-08-27 02:40:07.069 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc -   [3] INSERT public.t_stream_test BEFORE=- AFTER=[id=1406, payload=logback-smoke-3]
2026-08-27 02:40:07.069 [pgoutput-reader] INFO  o.vastdata.vbstream.cdc - TXN-END   xid=769
```

事务块头行的 `kind=` 区分形态：`NORMAL`（Begin..Commit）/ `STREAMED`（流式大事务）/ `TWO_PHASE`（两阶段，另带 gid）。回滚路径（StreamAbort 整事务、RollbackPrepared）不产生事务块，仅以生命周期控制消息留痕。

DML 统一带 `BEFORE=`/`AFTER=` 镜像（缺失侧为 `-`）。注意 BEFORE 的有无取决于表的 **replica identity**：默认（DEFAULT）下 UPDATE 仅在键列被修改时携带键元组、DELETE 携带键元组；`ALTER TABLE ... REPLICA IDENTITY FULL` 后 UPDATE/DELETE 恒携带变更前整行。

### 注意事项

- **流式触发与写入形态有关**：walsender 已追平时，单语句 `INSERT..SELECT` 批量写入的大事务不触发流式（整段于提交后一次性回放）；需要流式场景请事务内分批/跨秒写入，或调低 `logical_decoding_work_mem`
- **断线续传**：进程退出后槽保留，重启从最后确认的 LSN 续传；确认周期即 `feedbackSeconds`
- 手工清理槽：`SELECT pg_drop_replication_slot('vb_cdc_slot')`（先 `pg_terminate_backend(active_pid)` 若仍活跃）

## 测试

```bash
mvn test                # 全部：协议/组装单元测试 + Testcontainers 集成测试（151 用例）
mvn test -Dtest=StreamedTransactionTest    # 单类
```

集成测试（`org.vastdata.vbstream.it`，9 组）经 Testcontainers 自动起 postgres:18 容器（`logical_decoding_work_mem=64kB`），需本机 Docker。其中 `BenchCorpusRecordTest` 兼任 JMH 语料生成器——语料已提交进库且指纹一致时不启容器，常规 `mvn test` 秒级通过。

JMH 基准在独立源码根 `src/jmh`（`-Pjmh` 档才参与编译，默认构建零 JMH 依赖）；运行方式与基线数字见 `docs/benchmarks-baseline.md`。

## 路线

- 里程碑 1（完成）：pgoutput 解码器 + 复制会话 + Main/ConsoleListener
- 里程碑 1.5（完成）：raw 驱动的事务组装器——桶模型、StreamAbort 子事务剔除、2PC 挂起、Relation 版本日志 asOf 渲染
- 里程碑 1.6（完成）：组装缓冲溢写 Chronicle Queue——MEMORY/SPILLED 混合桶、低水位删档、瞬态工作区语义 + JMH 基线
- 里程碑 2（计划）：流式输出（边回放边吐出，替代提交期整桶物化）、输出队列、与 Logstash 集成
