# replication/ 模块——复制会话编排（IO 层）

职责：管理两条 JDBC 连接（普通 SQL + 复制专用），把 `protocol` 解码出的消息以回调形式交付上层。上游 `Main`/集成测试 `SessionHarness` 均只依赖 `PgReplicationSession` + `PgOutputListener`。

## PgReplicationSession（核心，AutoCloseable）

生命周期严格按 **open → ensureSlot → start → run → close**；`open` 失败会自行回收半开连接（防泄漏）。

- **`open()`**：建两条连接——普通 `config.jdbcUrl()` 与 `config.replicationUrl()`（带 `replication=database`）。复制连接要求见 ReplicationConfig。
- **`ensureSlot()`**：`SELECT pg_create_logical_replication_slot(槽名, 'pgoutput', false, twoPhase)` **幂等建槽**；捕获 SQLState `42710`（duplicate_object）视为"已存在，直接复用"并打 WARN（提示槽的 two_phase 属性需与配置一致，不一致将由 start 时服务端报错），其余异常上抛。
- **`start()`**：经 `PGConnection.getReplicationAPI()` 建 `PGReplicationStream`，slot options **四项**：`proto_version`、`publication_names`、`streaming`（OFF→"off"/ON→"on"/PARALLEL→"parallel"）、`two_phase`（on/off）；另经 `withStartPosition(INVALID_LSN)` 从槽当前确认点续传、`withStatusInterval` 设状态回传周期。
- **`run(listener)`**：**轮询式消息循环（100ms readPending 非阻塞轮询），由调用方线程执行**（Main/harness 中是名为 `pgoutput-reader` 的线程：harness 里为守护线程，Main 里为普通线程、靠 shutdown hook + CountDownLatch 收敛）。**每次调用新建** `PgOutputDecoder` 与 `RelationRegistry`（Relation 缓存与 inStream 状态不跨 run 存续）；声明 `throws SQLException, IOException`：
  1. 每轮先查 `stream.isClosed()`（断连快速感知，抛描述性 `SQLException`）；`stream.readPending()` 非阻塞取消息（null=暂无消息属正常，sleep 100ms 后继续）
  2. `PgOutputDecoder.decode()` → `RelationRegistry.accept()`（Relation 元数据入缓存）→ `listener.onMessage(msg, registry)` 回调
  3. 每轮 `setAppliedLSN/setFlushedLSN(getLastReceiveLSN())`；每满一个反馈周期 `forceUpdateStatus()` 上报确认位点
  - **为什么轮询而非阻塞 `read()`**（实测 pgjdbc 42.7.13 + PG 18）：阻塞 read 空闲期不按 statusInterval 醒来，status 依赖服务端 keepalive（~wal_sender_timeout/2，默认约 30s）才触发；轮询使 status 周期独立于消息到达（反馈间隔=feedbackIntervalSeconds，`pg_stat_replication.flush_lsn` 及时反映客户端进度）
  - **confirmed_flush_lsn 的服务端行为（Diag 实证，勿再当 bug 排查）**：standby status 到达后先被采纳进 `pg_stat_replication.flush_lsn`；槽的 `confirmed_flush_lsn` 由 walsender 在解码推进时（candidate 机制）落库——空闲期不推进，但确认不丢失，下一次任何 WAL 活动会使其一步跳到客户端已确认的最新位点。集成验证见 `NormalTransactionTest.feedbackIsAdoptedByServerAndConfirmedFlushAdvances`（两段式断言）
- **`close()`**：顺序 流 → 复制连接 → SQL 连接；关闭后 run 循环在下一轮 `isClosed()` 检查（≤100ms）抛 SQLException 退出。各步失败仅 WARN 不上抛。
- **`lastReceiveLsn()`**：供异常退出时打印续传位点；流未启动返回 INVALID_LSN。**`config()`**：暴露配置（harness 日志用）。

日志：生命周期 INFO（连接建立/槽已创建/流已启动/会话已关闭）、槽复用与关闭失败 WARN、LSN 反馈 DEBUG。

## ReplicationConfig（record，不可变）

11 个分量的配置模型；`fromSystemProperties()` 以 `vb.pg.*` 前缀读取系统属性，默认值对准 `src/docker` compose 环境（localhost:55432 / postgres 库 / 槽 vb_cdc_slot / publication vb_pub / proto 4 / streaming parallel / twoPhase true / 反馈 10s）。

- **`replicationUrl()`**：`jdbcUrl() + "?replication=database&assumeMinServerVersion=9.4"`——pgjdbc 规定 replication 连接必须同时带 `assumeMinServerVersion>=9.4` 才会把 replication 参数放进启动包，否则 `START_REPLICATION` 被服务端按普通 SQL 解析报语法错（真实 PG 18 首跑踩过）
- **`streamingParam()`**：StreamingMode → START_REPLICATION 参数值字符串

## RelationRegistry（oid → Relation 缓存）

协议保证 Relation 消息先于同表 DML 到达（含流式块内的重复下发）。

- `accept(message)`：只认 `Relation` 消息，按 `relationOid` 覆盖式 put（DDL 变更后服务端会重发新版本）
- `find(oid)`：`Optional<Relation>`，未命中返回 empty
- `require(oid)`：**未命中即抛 `IllegalStateException`（fail-fast）**——缓存 miss 意味着协议流异常而非数据缺失
- 写入方是 run 循环单线程，但 listener 回调可能从其他线程查询，故用 `ConcurrentHashMap`

## PgOutputListener（@FunctionalInterface 契约）

`onMessage(PgOutputMessage message, RelationRegistry registry)`：单方法消费者契约。**回调线程 = 调用 run() 的线程**（阻塞式，回调耗时直接拖慢消息循环与 LSN 反馈）。里程碑 2 的 Chronicle Queue 写入器实现此接口即可接入。

## 线程模型小结

`PgReplicationSession` 单实例**只服务一个 run 循环线程**（decoder 的 inStream 状态非线程安全）；`RelationRegistry` 是唯一为跨线程查询设计的类；回调在 run 线程内同步执行。
