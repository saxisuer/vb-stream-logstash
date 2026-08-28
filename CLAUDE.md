# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

vb-stream-logstash 是一个**全新（greenfield）项目**，目标：适配 PostgreSQL 最新的逻辑解码（logical decoding）**stream 模式**，通过 pgjdbc 的 `ReplicationConnection` / `PGReplicationStream` API 实时获取 CDC 数据。

- 坐标：`org.vastdata:vb-stream-logstash:1.0-SNAPSHOT`（Vastbase 生态；artifactId 暗示最终会以某种形式与 Logstash 集成，集成方式尚未确定）
- 工具链：Java 17 + Maven
- 当前状态：**里程碑 1.7 已完成**（在 pgoutput 流式解码器、复制会话（raw 字节接缝）与事务组装之上，**读取与组装输出解耦**：reader 线程只做记账（数据消息全量 append 进 Chronicle Queue 主缓冲 `MessagePipe`，桶只记 CQ index 段）+ 反馈 LSN；提交事务交接冻结桶（Relation 版本快照随行）由独立 `transaction-consumer` 线程回放输出；LSN 确认按**输出前沿**封顶（crash 丢失窗口为零，at-least-once）；JMH 基线含 1.7 管道口径（`docs/benchmarks-baseline.md`），`mvn test` 143 用例全绿）。核心依赖（版本以 pom 的 `<properties>` 为准）：
    - `org.postgresql:postgresql`（pgjdbc，含逻辑复制 API）
    - `net.openhft:chronicle-queue`（持久化低延迟队列——1.7 起是 reader 与 consumer 之间的**主缓冲管道**；会传递引入 chronicle-core/bytes/wire/threads 及 `slf4j-api`）
    - `ch.qos.logback:logback-classic`（slf4j 绑定；CDC 数据输出走专用 logger 名 `org.vastdata.vbstream.cdc`（INFO），解析层逐消息 DEBUG 默认关闭，配置在 `src/main/resources/logback.xml` 与 `src/test/resources/logback-test.xml`）

## 架构总览

端到端数据流（raw 接缝，里程碑 1.7 双线程解耦形态）——各组件机制详见对应包内 CLAUDE.md：

```
PostgreSQL 18（walsender 逻辑解码：pgoutput v4 + streaming + two_phase）
  │ CopyData/'w' WAL 帧（pgjdbc 已剥复制协议封装）
  ▼
PgReplicationSession.run()  ←100ms readPending 非阻塞轮询；LSN 确认 = min(已收到, 输出前沿)
  │ RawMessageListener.onRaw(byte[])：单条完整消息的独占数组（类型字节 + 流式块内可选 xid 前缀）
  ▼ reader 线程（pgoutput-reader）
TransactionAssembler  ←每条消息先 pipe.append 取 CQ index 作 seq → 按类型字节路由：
  │                   控制消息与 'R' live 解码；I/U/D/T/M 只窥前缀与 oid 记 index 段入桶
  │                   （解码推迟到 consumer 回放期——组装期桶内零字节引用）
  ├─ MessagePipe（Chronicle Queue 主缓冲：一条记录 = 一条完整消息，wipe-on-open）
  ├─ VersionedRelationRegistry：oid → (seq, Relation) 版本日志（DDL 重发同 oid 新版即追加）
  ▼ 提交期（Commit / StreamCommit / CommitPrepared）＝交接：拷快照 → 冻结桶 → 入交接队列 → 立即返回
  ▼ consumer 线程（transaction-consumer）
TransactionConsumer  ←交接队列取桶 → BucketReplayer 逐段 readRange（数据仅此一读）→ decodeSingle
  │                   → 按桶内 RelationSnapshot 的 asOf 版本渲染 → 前沿 AtomicLong ← endLsn
  │ TransactionListener.onTransaction(Transaction)：不可变原子事务块（consumer 线程回调）
  ▼
ConsoleListener（CDC 专用 logger org.vastdata.vbstream.cdc，INFO）
```

三层模块职责：

| 层 | 位置 | 职责 | 细节文档 |
|---|---|---|---|
| 协议解析 | `org.vastdata.vbstream.protocol` | pgoutput 消息字节 → 强类型 record，纯函数无 IO | `src/main/java/.../protocol/CLAUDE.md` |
| 会话与组装 | `org.vastdata.vbstream.replication` | 双 JDBC 连接、raw 字节交付接缝、解耦事务组装（reader 记账 + MessagePipe 管道 + consumer 回放） | `src/main/java/.../replication/CLAUDE.md` |
| 入口与输出 | `org.vastdata.vbstream`（顶层） | `Main` 装配、`ConsoleListener` 控制台输出 | 本节 |

- **`Main`**：冒烟入口（1.7 双线程装配）。校验配置（含 pipe 配置，非法值启动期 fail-fast）→ session open/ensureSlot/start → reader 线程（`pgoutput-reader`）内 try-with-resources 建**异步**组装器（独享 `VersionedRelationRegistry` 与 `PipeConfig`；构造即建管道并起非守护 `transaction-consumer` 线程；`ConsoleListener` 一个实例兼任事务回调与解码点 observer——组装器是唯一解码者，live 解码点传 registry、回放解码点传桶快照作渲染视图）→ `session.run(assembler, frontier::get)` 把 LSN 确认按输出前沿封顶 → 主线程 await 停机信号（Ctrl+C 触发 shutdown hook——hook 除 countDown 外还 join reader 线程，保证 JVM halt 前走完"run 退出 → 组装器毒丸排干"，已提交未输出的事务不丢）→ 关闭次序：会话 → 组装器（排干）→ 管道。启动失败 exit 1；复制流中断保留槽位并倒计时停机（重启续传）；consumer 回放失败经 onFailure 触发同一停机路径（fail-fast）
- **`ConsoleListener`**：双角色 listener，**两线程回调**（onMessage 的 live 分支在 reader、回放分支在 consumer；onTransaction 在 consumer）——自身无状态且 slf4j 线程安全。`onTransaction`：TXN-BEGIN/END 头尾 + 逐变更行，基于 `TxChange` 内嵌 Relation 快照渲染（不依赖 registry），INFO；`onMessage`：9 种事务生命周期控制消息（流式 5 + 两阶段 4）升 INFO，行级/元数据 DEBUG（默认关闭）——INFO 级保证任何事务形态至少留一行痕迹。值渲染：text 截 64 字符、binary 十六进制、TOAST 未变显式标注

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
#           -Dvb.pipe.dir=... -Dvb.pipe.rollCycle=...
```

- **`--add-opens` 清单必带**：Main 装配的 `TransactionAssembler` 构造即建 Chronicle Queue 管道（`MessagePipe`——1.7 起是主缓冲，不再是"越过阈值才溢写"），chronicle-core 的 mmap 在 Java 17 需开放内部包（反射调 `sun.nio.ch.FileChannelImpl.map0`，官方支持说明 https://chronicle.software/chronicle-support-java-17）；清单与 pom 的 surefire argLine 同源
- **pipe 参数（`-Dvb.pipe.*`，`PipeConfig`，默认值即下表）**：

| 属性 | 默认 | 语义 |
|---|---|---|
| `vb.pipe.dir` | `pipe-queue` | 管道队列目录（相对工作目录） |
| `vb.pipe.rollCycle` | `MINUTELY` | 滚动周期（`LegacyRollCycles` 枚举名，大小写宽容） |

- **管道目录重启自动清空属预期行为**：管道是瞬态工作区——真源是复制槽，重启后 PG 从确认位点（输出前沿封顶值）重发未输出事务，`MessagePipe` 构造时先整体清空目录再建队列（残留旧数据的有害陈旧 index 会让回读错位）。不要往该目录放任何需要保留的东西。同 JVM 第二个管道实例指向同一目录会清掉前者的队列文件（进程内独占）
- **内存有界性（1.7 形态）**：**组装期堆内零字节引用**——数据消息字节只在 `pipe.append` 时落盘一次，桶只记 CQ index 段（段数 × long[2]）与 oid/aborted 集合；回放期（consumer 线程）把整桶单元从管道读回堆，原始字节与解码出的 TxChange 双份瞬态并存，峰值 O(事务大小)——流式输出（边回放边吐出）属里程碑 2 范畴，1.7 的解耦只把这段瞬态从 reader 路径移到 consumer 线程，不改变其量级。仍随事务/会话增长的堆结构：`abortedSubxids`（每回滚子事务一个 Long，随桶完结释放）、`preparedByGid` 挂起池（未决 2PC 数，协议固有）、交接队列与 `handedOff` 记账（待输出桶数 × 元数据，DONE 惰性清理）、registry 版本日志（随新表/DDL 线性——组装器在桶完结点按存活桶 firstIndex 低水位 `pruneBelow` 剪枝，floor 语义，2PC 挂起桶算存活、已交接桶不算；剪枝后仅随不同表 oid 数线性）。consumer 慢/停摆不回压 reader，代价转移到磁盘：CQ 目录与 PG 侧 WAL 保留增长（`max_slot_wal_keep_size=2GB` 兜底 + consumer 周期 WARN 告警）
- **输出语义**：at-least-once——LSN 确认按输出前沿封顶，crash 时未输出事务必然被 PG 重发，console 可能重复输出已见事务（不去重，文档化承诺）
- **源码结构**（各源码根一行；包内细节见各模块级 CLAUDE.md，层间关系见上文“架构总览”）：
    - `src/main/java`：`protocol`（协议解析，纯函数）、`replication`（会话 + raw 接缝 + 解耦事务组装与管道）、顶层 `Main`/`ConsoleListener`
    - `src/test/java`：`protocol`/`replication` 包字节级单测（`MsgBuilder`/`PgWire` 手造字节辅助）与顶层 `ConsoleListenerTest`、`it` 包集成测试 11 组（Testcontainers，见其 CLAUDE.md）、`bench` 包语料基建（JMH 语料来源）
    - `src/jmh/java`：四基准（`-Pjmh` 档才参与编译，默认构建零 JMH 依赖，见其 CLAUDE.md）
- 集成测试（`org.vastdata.vbstream.it`，11 组）经 Testcontainers 自动起 postgres:18 容器，需本机 Docker；`mvn test` 单命令跑全部。解耦专项三组：`DecoupledPipelineTest` 三场景（①双连接并发流式大事务多桶交错 + StreamAbort 子事务剔除，异步管道双回放输出全等 ②大事务内同事务 DDL，前后段按 asOf 版本渲染 ③流式大事务回滚后低水位推进 + 陈旧滚动文件实际删档）、`ReaderUnblockedTest`（头名验收——consumer 阻塞期间 reader 持续接收，放行后排干不丢不重）、`FrontierCapTest`（未输出事务钉住 confirmed_flush，输出后越过封顶）；`BenchCorpusRecordTest` 为基准语料生成器（语料缺失或场景脚本 SHA-256 指纹变化才起容器重录，指纹一致时秒过）
- JMH 基准运行方式见 `docs/benchmarks-baseline.md`（须在模块根目录运行）：`mvn -Pjmh clean test-compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt` 后 `java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" org.openjdk.jmh.Main "org.vastdata.vbstream.bench" ...`（JMH fork 是全新 JVM，`--add-opens` 须经 `-jvmArgsAppend` 自带，详见该文档；基线数字在档作回归对照——1.7 段 + 1.6 历史参照，不进 CI）
- src/docker 的 postgresql.conf 已含冒烟所需 `max_prepared_transactions=16` 与 `logical_decoding_work_mem=64kB`（改 conf 后 `docker compose restart postgres`）。注意：walsender 已追平时，单语句 `INSERT..SELECT` 批量写入的大事务不触发流式（整段于提交后回放）；构造流式场景需事务内分批/跨秒写入
- **流式驱逐的内存记账按 TOAST 压缩后大小（实测，构造流式测试数据必读）**：reorder buffer 的 `rb->size` 按变更元组 TOAST 压缩后的实际字节数记账，不是 SQL 文本长度。规则图案载荷（`repeat('x',8192)`、`repeat(md5,N)`）被 pglz 压到百字节级——少量行永远越不过 `logical_decoding_work_mem=64kB`，事务整体走 Begin..Commit 的 NORMAL 路径（事务组装 Task 8 首版实测踩坑）。要少量行即触发流式，用不可压缩载荷：`(SELECT string_agg(md5(random()::text),'') FROM generate_series(1,512))` ≈16KB（`pg_column_size` 实测存满 16384）；数百行可压缩载荷靠总量也能触发（`StreamedTransactionTest` 的 500 行方案）。另注意阈值是**全局** `rb->size`（所有进行中事务合计），双连接并发大事务会轮番驱逐、流段交错下发（`TransactionAssemblyTest` 场景 4 即此构造）

## 领域要点（实现时的关键约束）

- 目标是 stream 模式（**流式发送进行中的大事务**），而不是等事务提交后整体回放的传统模式。需要 PG 14+（复制槽 `streaming` 选项），pgoutput 插件需 `proto_version >= 2` 才会收到流式消息。
- pgjdbc 复制 API 的入口链路（42.7.13 实际签名）：JDBC URL 带 `replication=database` **且必须 `assumeMinServerVersion=9.4`**（否则 replication 参数被驱动静默丢弃，START_REPLICATION 报语法错）→ `PGConnection.getReplicationAPI()`（返回 `PGReplicationConnection`）→ `replicationStream().logical()` 建流（slot options：`proto_version`/`publication_names`/`streaming`/`two_phase`）；本项目建槽走 SQL `pg_create_logical_replication_slot`。消费循环用非阻塞 `readPending()` 轮询（阻塞 `read()` 空闲期不按 statusInterval 醒来），周期 `setAppliedLSN()/setFlushedLSN()` + `forceUpdateStatus()` 回传确认。
- **confirmed_flush_lsn 的服务端行为（Diag 实证，勿当 bug 排查）**：standby status 先被服务端采纳进 `pg_stat_replication.flush_lsn`；槽的 `confirmed_flush_lsn` 由 walsender 在**解码推进时**（candidate 机制）落库——空闲期不推进但确认不丢失，下一次任何 WAL 活动会使其一步跳到客户端已确认的最新位点；`max_slot_wal_keep_size` 兜底磁盘。1.7 起客户端确认值另经输出前沿封顶（`FrontierCapTest` 验收）。
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
