# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

vb-stream-logstash 是一个**全新（greenfield）项目**，目标：适配 PostgreSQL 最新的逻辑解码（logical decoding）**stream 模式**，通过 pgjdbc 的 `ReplicationConnection` / `PGReplicationStream` API 实时获取 CDC 数据。

- 坐标：`org.vastdata:vb-stream-logstash:1.0-SNAPSHOT`（Vastbase 生态；artifactId 暗示最终会以某种形式与 Logstash 集成，集成方式尚未确定）
- 工具链：Java 17 + Maven
- 当前状态：**里程碑 1.6 已完成**（在 pgoutput 流式解码器、复制会话（raw 字节接缝）与事务组装之上，组装缓冲溢写 Chronicle Queue：`TransactionAssembler` 的 MEMORY/SPILLED 混合桶 + `MessageSpool` 溢写池 + `VersionedRelationRegistry` 版本日志 + JMH 基线（`docs/benchmarks-baseline.md`），`mvn test` 151 用例全绿）。核心依赖（版本以 pom 的 `<properties>` 为准）：
    - `org.postgresql:postgresql`（pgjdbc，含逻辑复制 API）
    - `net.openhft:chronicle-queue`（持久化低延迟队列；会传递引入 chronicle-core/bytes/wire/threads 及 `slf4j-api`）
    - `ch.qos.logback:logback-classic`（slf4j 绑定；CDC 数据输出走专用 logger 名 `org.vastdata.vbstream.cdc`（INFO），解析层逐消息 DEBUG 默认关闭，配置在 `src/main/resources/logback.xml` 与 `src/test/resources/logback-test.xml`）

## 架构总览

端到端数据流（raw 接缝，里程碑 1.6 形态）——各组件机制详见对应包内 CLAUDE.md：

```
PostgreSQL 18（walsender 逻辑解码：pgoutput v4 + streaming + two_phase）
  │ CopyBoth 消息（pgjdbc 已剥复制协议头）
  ▼
PgReplicationSession.run()  ←100ms readPending 非阻塞轮询；周期回传 LSN 确认
  │ RawMessageListener.onRaw(byte[])：单条完整消息的独占数组（类型字节 + 流式块内可选 xid 前缀）
  ▼
TransactionAssembler  ←全局 seq 分配 → 按类型字节路由：控制消息与 'R' live 解码；
  │                   I/U/D/T/M 只窥 xid 前缀构造 PayloadUnit 入桶（解码推迟到提交期）
  ├─ MEMORY 桶（默认）──字节和越 vb.spill.thresholdBytes──▶ spillAll() 全量转储
  │                                                        └▶ MessageSpool（Chronicle Queue 溢写池）
  ├─ VersionedRelationRegistry：oid → (seq, Relation) 版本日志（DDL 重发同 oid 新版即追加）
  ▼ 提交期（Commit / StreamCommit / CommitPrepared）
BucketReplayer  ←aborted 子事务过滤 → decodeSingle → 按单元 seq 取 asOf 版本 Relation 渲染
  │ TransactionListener.onTransaction(Transaction)：不可变原子事务块
  ▼
ConsoleListener（CDC 专用 logger org.vastdata.vbstream.cdc，INFO）
```

三层模块职责：

| 层 | 位置 | 职责 | 细节文档 |
|---|---|---|---|
| 协议解析 | `org.vastdata.vbstream.protocol` | pgoutput 消息字节 → 强类型 record，纯函数无 IO | `src/main/java/.../protocol/CLAUDE.md` |
| 会话与组装 | `org.vastdata.vbstream.replication` | 双 JDBC 连接、raw 字节交付接缝、事务组装（MEMORY/SPILLED 混合桶 + 溢写） | `src/main/java/.../replication/CLAUDE.md` |
| 入口与输出 | `org.vastdata.vbstream`（顶层） | `Main` 装配、`ConsoleListener` 控制台输出 | 本节 |

- **`Main`**：冒烟入口。校验配置（缺失 exit 2 打用法）→ session open/ensureSlot/start → reader 线程（`pgoutput-reader`）内 try-with-resources 建组装器（独享 `VersionedRelationRegistry` 与 `SpillConfig`；`ConsoleListener` 一个实例兼任事务回调与解码点 observer——组装器是唯一解码者）→ 主线程 await 停机信号（Ctrl+C 触发 shutdown hook）→ 会话关闭使 run 退出、组装器随之收尾 spill 池。启动失败 exit 1；复制流中断保留槽位并倒计时停机（重启续传）
- **`ConsoleListener`**：双角色 listener。`onTransaction`：TXN-BEGIN/END 头尾 + 逐变更行，基于 `TxChange` 内嵌 Relation 快照渲染（不依赖 registry），INFO；`onMessage`：9 种事务生命周期控制消息（流式 5 + 两阶段 4）升 INFO，行级/元数据 DEBUG（默认关闭）——INFO 级保证任何事务形态至少留一行痕迹。值渲染：text 截 64 字符、binary 十六进制、TOAST 未变显式标注

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

## 开发规约

- **日志输出一律走 slf4j**（`private static final Logger LOG = LoggerFactory.getLogger(Xxx.class)`），**禁止 `System.out` / `System.err`**（主代码与测试代码均适用；临时调试打印不得提交）：
  - 级别语义：CDC 数据输出走专用 logger `org.vastdata.vbstream.cdc`（INFO，与系统日志分离、可独立调级）——事务块与**事务生命周期控制消息**（流式 StreamStart/Stop/Commit/Abort/Prepare + 两阶段信号，共 9 种）用 INFO，保证任何事务形态（含回滚/无组装块路径）在 INFO 级至少留一行痕迹；生命周期/状态变更用 INFO，可恢复异常用 WARN，失败/退出路径用 ERROR，行级/元数据逐消息细节用 DEBUG（默认关闭，大事务防刷屏）
  - 消息用 `{}` 占位符拼参（不做 `+` 字符串拼接，避免无效格式化开销）；异常对象作为最后一个参数传入以保留堆栈
- **每个函数必须有 javadoc 逻辑描述**（含私有方法与测试辅助方法），不得只复述方法名：
  - 说清**职责**（做什么）、**关键步骤**（分支/循环/算法的意图，复杂逻辑分步）、**边界与异常语义**（null/失败/越界时的行为）、**线程约束**（非线程安全需注明单写者假设，参照 `PgOutputDecoder` 的写法）
  - record 组件、常量、枚举值同样注明语义；协议相关代码需指向依据（如"格式见 spec 附录 A"），现状范例参照 `protocol/` 包

## 运行 Main

```bash
cd src/docker && docker compose up -d && cd ../..     # 起本地 PG
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED \
     --add-opens java.base/sun.nio.fs=ALL-UNNAMED \
     --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp "target/classes:$(cat target/cp.txt)" org.vastdata.vbstream.Main
# 可选覆盖：-Dvb.pg.slot=... -Dvb.pg.publication=... -Dvb.pg.streaming=on|parallel|off
#           -Dvb.spill.thresholdBytes=... -Dvb.spill.dir=... -Dvb.spill.rollCycle=...
```

- **`--add-opens` 清单必带**：Main 装配的 `TransactionAssembler` 越过 spill 阈值时会建 Chronicle Queue 溢写池，chronicle-core 的 mmap 在 Java 17 需开放内部包（反射调 `sun.nio.ch.FileChannelImpl.map0`，官方支持说明 https://chronicle.software/chronicle-support-java-17）；清单与 pom 的 surefire argLine 同源
- **spill 参数（`-Dvb.spill.*`，`SpillConfig`，默认值即下表）**：

| 属性 | 默认 | 语义 |
|---|---|---|
| `vb.spill.thresholdBytes` | 67108864（64MiB） | 全局 MEMORY 桶字节和阈值，越限即整体转储溢写池；**≤0 = 禁用 spill**（纯内存逃生门，回退里程碑 1.5 行为） |
| `vb.spill.dir` | `spill-queue` | 溢写队列目录（相对工作目录） |
| `vb.spill.rollCycle` | `MINUTELY` | 滚动周期（`LegacyRollCycles` 枚举名，大小写宽容） |

- **spill 队列目录重启自动清空属预期行为**：溢写池是瞬态工作区——真源是复制槽，重启后 PG 从槽确认位点重发未完事务，`MessageSpool` 构造时先整体清空目录再建队列（残留旧数据的有害陈旧 index 会让回读错位）。不要往该目录放任何需要保留的东西
- **spill 的内存有界性只覆盖组装期**（终审修正）：阈值约束的是进行中事务的桶缓冲；提交期回放把整桶单元物化回堆——SPILLED 桶逐段回读的原始字节与回放解码出的 TxChange 双份瞬态并存，峰值 O(事务大小)；流式输出（边回放边吐出）属里程碑 2 范畴。仍随事务/会话增长的堆结构：`abortedSubxids`（每回滚子事务一个 Long，随桶完结释放）、`preparedByGid` 挂起池（未决 2PC 数，协议固有）、registry 版本日志（随新表/DDL 线性；组装器在桶完结点按存活桶 minSeq 低水位 `pruneBelow` 剪枝——floor 语义，保留低水位时刻生效的版本，2PC 挂起桶算存活；剪枝后仅随不同表 oid 数线性）
- **源码结构**（各源码根一行；包内细节见各模块级 CLAUDE.md，层间关系见上文“架构总览”）：
    - `src/main/java`：`protocol`（协议解析，纯函数）、`replication`（会话 + raw 接缝 + 事务组装与溢写）、顶层 `Main`/`ConsoleListener`
    - `src/test/java`：`protocol`/`replication` 包字节级单测（`MsgBuilder`/`PgWire` 手造字节辅助）、`it` 包集成测试 9 组（Testcontainers，见其 CLAUDE.md）、`bench` 包语料基建（JMH 语料来源）
    - `src/jmh/java`：四基准（`-Pjmh` 档才参与编译，默认构建零 JMH 依赖，见其 CLAUDE.md）
- 集成测试（`org.vastdata.vbstream.it`，9 组）经 Testcontainers 自动起 postgres:18 容器，需本机 Docker；`mvn test` 单命令跑全部。溢写专项 `AssemblySpillTest` 四场景：①同录制字节流喂 64MiB/32KiB 双阈值组装器，输出 `Transaction` 全等（spill 无损核心验收）②双连接并发流式大事务多桶交错 + StreamAbort 子事务剔除 ③大事务内同事务 DDL，前后段按 asOf 版本渲染 ④流式大事务回滚后低水位推进触发删档；`BenchCorpusRecordTest` 为基准语料生成器（语料缺失或场景脚本 SHA-256 指纹变化才起容器重录，指纹一致时秒过）
- JMH 基准运行方式见 `docs/benchmarks-baseline.md`（须在模块根目录运行）：`mvn -Pjmh clean test-compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt` 后 `java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" org.openjdk.jmh.Main "org.vastdata.vbstream.bench" ...`（JMH fork 是全新 JVM，`--add-opens` 须经 `-jvmArgsAppend` 自带，详见该文档；基线数字在档作回归对照，不进 CI）
- src/docker 的 postgresql.conf 已含冒烟所需 `max_prepared_transactions=16` 与 `logical_decoding_work_mem=64kB`（改 conf 后 `docker compose restart postgres`）。注意：walsender 已追平时，单语句 `INSERT..SELECT` 批量写入的大事务不触发流式（整段于提交后回放）；构造流式场景需事务内分批/跨秒写入
- **流式驱逐的内存记账按 TOAST 压缩后大小（实测，构造流式测试数据必读）**：reorder buffer 的 `rb->size` 按变更元组 TOAST 压缩后的实际字节数记账，不是 SQL 文本长度。规则图案载荷（`repeat('x',8192)`、`repeat(md5,N)`）被 pglz 压到百字节级——少量行永远越不过 `logical_decoding_work_mem=64kB`，事务整体走 Begin..Commit 的 NORMAL 路径（事务组装 Task 8 首版实测踩坑）。要少量行即触发流式，用不可压缩载荷：`(SELECT string_agg(md5(random()::text),'') FROM generate_series(1,512))` ≈16KB（`pg_column_size` 实测存满 16384）；数百行可压缩载荷靠总量也能触发（`StreamedTransactionTest` 的 500 行方案）。另注意阈值是**全局** `rb->size`（所有进行中事务合计），双连接并发大事务会轮番驱逐、流段交错下发（`TransactionAssemblyTest` 场景 4 即此构造）

## 领域要点（实现时的关键约束）

- 目标是 stream 模式（**流式发送进行中的大事务**），而不是等事务提交后整体回放的传统模式。需要 PG 14+（复制槽 `streaming` 选项），pgoutput 插件需 `proto_version >= 2` 才会收到流式消息。
- pgjdbc 复制 API 的入口链路（42.7.13 实际签名）：JDBC URL 带 `replication=database` **且必须 `assumeMinServerVersion=9.4`**（否则 replication 参数被驱动静默丢弃，START_REPLICATION 报语法错）→ `PGConnection.getReplicationAPI()`（返回 `PGReplicationConnection`）→ `replicationStream().logical()` 建流（slot options：`proto_version`/`publication_names`/`streaming`/`two_phase`）；本项目建槽走 SQL `pg_create_logical_replication_slot`。消费循环用非阻塞 `readPending()` 轮询（阻塞 `read()` 空闲期不按 statusInterval 醒来），周期 `setAppliedLSN()/setFlushedLSN()` + `forceUpdateStatus()` 回传确认。
- **confirmed_flush_lsn 的服务端行为（Diag 实证，勿当 bug 排查）**：standby status 先被服务端采纳进 `pg_stat_replication.flush_lsn`；槽的 `confirmed_flush_lsn` 由 walsender 在**解码推进时**（candidate 机制）落库——空闲期不推进但确认不丢失，下一次任何 WAL 活动会使其一步跳到客户端已确认的最新位点；`max_slot_wal_keep_size` 兜底磁盘。
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
